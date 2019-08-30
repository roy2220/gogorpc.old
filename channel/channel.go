package channel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/let-z-go/intrusives/list"
	"github.com/let-z-go/toolkit/deque"
	"github.com/let-z-go/toolkit/utils"
	"github.com/let-z-go/toolkit/uuid"

	"github.com/let-z-go/pbrpc/internal/protocol"
	"github.com/let-z-go/pbrpc/internal/stream"
	"github.com/let-z-go/pbrpc/internal/transport"
)

type Channel struct {
	options                *Options
	state                  int32
	failedEventType        EventType
	listenerManager        listenerManager
	dequeOfPendingRequests deque.Deque
	stream                 unsafe.Pointer
	nextSequenceNumber     uint32
	pendingRPCs            sync.Map
	pendingAbort           atomic.Value
}

func (self *Channel) Init(options *Options) *Channel {
	self.options = options.Normalize()
	self.state = int32(Initial)
	self.dequeOfPendingRequests.Init(0)
	self.stream = unsafe.Pointer(new(stream.Stream).Init(self.options.Stream, &self.dequeOfPendingRequests, nil))
	return self
}

func (self *Channel) Close() {
	self.setState(Closed, self.failedEventType)
}

func (self *Channel) AddListener(normalNumberOfEvents int) (*Listener, error) {
	return self.listenerManager.AddListener(func() error {
		return self.getClosedError()
	}, normalNumberOfEvents)
}

func (self *Channel) RemoveListener(listener *Listener) error {
	return self.listenerManager.RemoveListener(func() error {
		return self.getClosedError()
	}, listener)
}

func (self *Channel) InvokeRPC(rpc *RPC) {
	if parentRPC, ok := GetRPC(rpc.Ctx); ok {
		rpc.internals.TraceID = parentRPC.internals.TraceID
	} else {
		rpc.internals.TraceID = uuid.GenerateUUID4Fast()
	}

	methodOptions := self.options.GetMethod(rpc.ServiceName, rpc.MethodName)

	if self.isClosed() {
		rpc.internals.Init(func(rpc *RPC) {
			rpc.Err = ErrClosed
		}, methodOptions.OutgoingRPCInterceptors)

		rpc.Ctx = BindRPC(rpc.Ctx, rpc)
		rpc.Handle()
		return
	}

	rpc.internals.SequenceNumber = int32(self.getNextSequenceNumber())

	if deadline, ok := rpc.Ctx.Deadline(); ok {
		rpc.internals.Deadline = deadline.UnixNano()
	} else {
		rpc.internals.Deadline = 0
	}

	rpc.internals.Init(func(rpc *RPC) {
		pendingRPC_ := getPooledPendingRPC().Init(methodOptions.ResponseFactory)
		self.pendingRPCs.Store(rpc.internals.SequenceNumber, pendingRPC_)

		if err := self.getStream().SendRequest(rpc.Ctx, &protocol.RequestHeader{
			SequenceNumber: rpc.internals.SequenceNumber,
			ServiceName:    rpc.ServiceName,
			MethodName:     rpc.MethodName,
			ExtraData:      rpc.RequestExtraData,
			Deadline:       rpc.internals.Deadline,

			TraceId: protocol.UUID{
				Low:  rpc.internals.TraceID[0],
				High: rpc.internals.TraceID[1],
			},
		}, rpc.Request); err != nil {
			self.pendingRPCs.Delete(rpc.internals.SequenceNumber)

			switch err {
			case stream.ErrClosed:
				rpc.Err = ErrClosed
			default:
				rpc.Err = err
			}

			return
		}

		if err := pendingRPC_.WaitFor(rpc.Ctx); err != nil {
			rpc.Err = err
			return
		}

		if pendingRPC_.Err == stream.ErrRequestExpired {
			<-rpc.Ctx.Done()
			pendingRPC_.Err = rpc.Ctx.Err()
		}

		rpc.ResponseExtraData = pendingRPC_.ResponseExtraData
		rpc.Response = pendingRPC_.Response
		rpc.Err = pendingRPC_.Err
		putPooledPendingRPC(pendingRPC_)
	}, methodOptions.OutgoingRPCInterceptors)

	rpc.Ctx = BindRPC(rpc.Ctx, rpc)
	rpc.Handle()
}

func (self *Channel) Abort(extraData ExtraData) {
	self.pendingAbort.Store(extraData)
	self.getStream().Abort(extraData)
}

func (self *Channel) AcceptAndServe(ctx context.Context, connection net.Conn) error {
	err := func() error {
		if err := self.accept(ctx, connection); err != nil {
			self.options.Logger().Error().Err(err).
				Str("transport_id", self.getTransportID().String()).
				Msg("channel_accept_failed")
			return err
		}

		if err := self.serve(ctx); err != nil {
			self.options.Logger().Error().Err(err).
				Str("transport_id", self.getTransportID().String()).
				Msg("channel_serve_failed")
			return err
		}

		return nil
	}()

	self.Close()
	return err
}

func (self *Channel) ConnectAndServe(ctx context.Context, serverAddressProvider ServerAddressProvider) error {
	connectRetryCount := -1
	err := error(nil)

	for {
		connectRetryCount++
		var serverAddress string
		serverAddress, err = serverAddressProvider(ctx, connectRetryCount)

		if err != nil {
			break
		}

		err = func() error {
			if err := self.connect(ctx, serverAddress); err != nil {
				self.options.Logger().Error().Err(err).
					Str("transport_id", self.getTransportID().String()).
					Msg("channel_connect_failed")
				return err
			}

			connectRetryCount = -1

			if err := self.serve(ctx); err != nil {
				self.options.Logger().Error().Err(err).
					Str("transport_id", self.getTransportID().String()).
					Msg("channel_serve_failed")
				return err
			}

			return nil
		}()

		if err2 := ctx.Err(); err2 != nil {
			err = err2
			break
		}

		if _, ok := err.(*transport.NetworkError); !ok {
			break
		}
	}

	self.Close()
	return err
}

func (self *Channel) accept(ctx context.Context, connection net.Conn) error {
	self.setState(Establishing, EventAccepting)
	ok, err := self.getStream().Accept(ctx, connection, self.options.Handshaker)

	if err != nil {
		self.failedEventType = EventAcceptFailed
		return err
	}

	if !ok {
		self.failedEventType = EventAcceptFailed
		return ErrHandshakeRefused
	}

	self.setState(Established, EventAccepted)
	return nil
}

func (self *Channel) connect(ctx context.Context, serverAddress string) error {
	transportID := self.getStream().GetTransportID()
	self.setState(Establishing, EventConnecting)
	ok, err := self.getStream().Connect(ctx, serverAddress, transportID, self.options.Handshaker)

	if err != nil {
		self.failedEventType = EventConnectFailed
		return err
	}

	if !ok {
		self.failedEventType = EventConnectFailed
		return ErrHandshakeRefused
	}

	self.setState(Established, EventConnected)
	return nil
}

func (self *Channel) serve(ctx context.Context) error {
	stream_ := self.getStream()

	err := stream_.Serve(ctx, &messageProcessor{
		Options:     self.options,
		Stream:      stream_,
		PendingRPCs: &self.pendingRPCs,
	})

	self.failedEventType = EventServeFailed
	return err
}

func (self *Channel) setState(newState State, eventType EventType) {
	oldState := self.getState()
	stateTransitionIsValid := false
	err := error(nil)

	switch oldState {
	case Initial:
		switch newState {
		case Establishing:
			stateTransitionIsValid = true
		case Closed:
			stateTransitionIsValid = true
			err = ErrClosed
		}
	case Establishing:
		switch newState {
		case Establishing:
			stateTransitionIsValid = true
			err = ErrBroken
		case Established:
			stateTransitionIsValid = true
		case Closed:
			stateTransitionIsValid = true
			err = ErrClosed
		}
	case Established:
		switch newState {
		case Establishing:
			stateTransitionIsValid = true
			err = ErrBroken
		case Closed:
			stateTransitionIsValid = true
			err = ErrClosed
		}
	}

	utils.Assert(stateTransitionIsValid, func() string {
		return fmt.Sprintf("pbrpc/channel: invalid state transition: oldState=%#v, newState=%#v, eventType=%#v", oldState, newState, eventType)
	})

	if newState != oldState {
		atomic.StoreInt32(&self.state, int32(newState))
		logEvent := self.options.Logger().Info()

		if transportID := self.getTransportID(); !transportID.IsZero() {
			logEvent.Str("transport_id", transportID.String())
		}

		logEvent.Str("old_state", oldState.GoString()).
			Str("new_state", newState.GoString()).
			Str("event_type", eventType.GoString()).
			Msg("channel_state_transition")
		self.listenerManager.FireEvent(eventType, oldState, newState)
	}

	if err != nil {
		if err == ErrBroken {
			oldStream := self.getStream()
			newStream := new(stream.Stream).Init(self.options.Stream, &self.dequeOfPendingRequests, nil)

			if value := self.pendingAbort.Load(); value != nil {
				newStream.Abort(value.(ExtraData))
			}

			atomic.StorePointer(&self.stream, unsafe.Pointer(newStream))
			oldStream.Close()

			self.pendingRPCs.Range(func(key interface{}, value interface{}) bool {
				pendingRPC_ := value.(*pendingRPC)

				if pendingRPC_.IsEmitted {
					self.pendingRPCs.Delete(key)
					pendingRPC_.Fail(nil, err)
				}

				return true
			})
		} else { // err == ErrClosed
			self.listenerManager.Close()
			self.getStream().Close()
			listOfPendingRequests := new(list.List).Init()
			self.dequeOfPendingRequests.Close(listOfPendingRequests)
			stream.PutPooledPendingRequests(listOfPendingRequests)

			self.pendingRPCs.Range(func(key interface{}, value interface{}) bool {
				self.pendingRPCs.Delete(key)
				pendingRPC_ := value.(*pendingRPC)

				if pendingRPC_.IsEmitted {
					pendingRPC_.Fail(nil, ErrBroken)
				} else {
					pendingRPC_.Fail(nil, err)
				}

				return true
			})
		}
	}
}

func (self *Channel) getState() State {
	return State(atomic.LoadInt32(&self.state))
}

func (self *Channel) getClosedError() error {
	state := self.getState()
	return state2ClosedError[state]
}

func (self *Channel) isClosed() bool {
	return self.getClosedError() != nil
}

func (self *Channel) getStream() *stream.Stream {
	return (*stream.Stream)(atomic.LoadPointer(&self.stream))
}

func (self *Channel) getTransportID() uuid.UUID {
	return self.getStream().GetTransportID()
}

func (self *Channel) getNextSequenceNumber() int {
	return int((atomic.AddUint32(&self.nextSequenceNumber, 1) - 1) & 0x7FFFFFFF)
}

type ServerAddressProvider func(ctx context.Context, connectRetryCount int) (string, error)

var ErrHandshakeRefused = errors.New("pbrpc/channel: handshake refused")
var ErrBroken = errors.New("pbrpc/channel: broken")
var ErrClosed = errors.New("pbrpc/channel: closed")

var state2ClosedError = [...]error{
	None:         ErrClosed,
	Initial:      nil,
	Establishing: nil,
	Established:  nil,
	Closed:       ErrClosed,
}
