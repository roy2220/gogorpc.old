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
)

type Channel struct {
	options                *Options
	state                  int32
	dequeOfPendingRequests deque.Deque
	stream                 unsafe.Pointer
	nextSequenceNumber     uint32
	pendingRPCs            sync.Map
	pendingAbort           atomic.Value
}

func (self *Channel) Init(options *Options) *Channel {
	self.options = options.Normalize()
	self.state = int32(initial)
	self.dequeOfPendingRequests.Init(0)
	self.stream = unsafe.Pointer(new(stream.Stream).Init(self.options.Stream, &self.dequeOfPendingRequests, nil))
	return self
}

func (self *Channel) Close() {
	self.setState(closed)
}

func (self *Channel) Accept(ctx context.Context, connection net.Conn) error {
	self.setState(establishing)
	ok, err := self.getStream().Accept(ctx, connection, self.options.Handshaker)

	if err != nil {
		return err
	}

	if !ok {
		return ErrHandshakeRefused
	}

	return nil
}

func (self *Channel) Connect(ctx context.Context, connection net.Conn) error {
	transportID := self.getStream().GetTransportID()
	self.setState(establishing)
	ok, err := self.getStream().Connect(ctx, connection, transportID, self.options.Handshaker)

	if err != nil {
		return err
	}

	if !ok {
		return ErrHandshakeRefused
	}

	return nil
}

func (self *Channel) Process(ctx context.Context) error {
	self.setState(established)
	stream_ := self.getStream()

	err := stream_.Process(ctx, &messageProcessor{
		Options:     self.options,
		Stream:      stream_,
		PendingRPCs: &self.pendingRPCs,
	})

	if hangupError, ok := err.(*stream.HangupError); ok && hangupError.IsPassive {
		self.options.AbortHandler(ctx, hangupError.ExtraData)
	}

	return err
}

func (self *Channel) InvokeRPC(rpc *RPC, responseFactory MessageFactory) {
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
		pendingRPC_ := getPooledPendingRPC().Init(responseFactory)
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

func (self *Channel) setState(newState state) {
	oldState := self.getState()
	stateTransitionIsValid := false
	err := error(nil)

	switch oldState {
	case initial:
		switch newState {
		case establishing:
			stateTransitionIsValid = true
		case closed:
			stateTransitionIsValid = true
			err = ErrClosed
		}
	case establishing:
		switch newState {
		case establishing:
			stateTransitionIsValid = true
			err = ErrBroken
		case established:
			stateTransitionIsValid = true
		case closed:
			stateTransitionIsValid = true
			err = ErrClosed
		}
	case established:
		switch newState {
		case establishing:
			stateTransitionIsValid = true
			err = ErrBroken
		case closed:
			stateTransitionIsValid = true
			err = ErrClosed
		}
	}

	utils.Assert(stateTransitionIsValid, func() string {
		return fmt.Sprintf("pbrpc/channel: invalid state transition: oldState=%#v, newState=%#v", oldState, newState)
	})

	if newState != oldState {
		atomic.StoreInt32(&self.state, int32(newState))
		logEvent := self.options.Logger.Info()

		if transportID := self.getStream().GetTransportID(); !transportID.IsZero() {
			logEvent.Str("transport_id", transportID.String())
		}

		logEvent.Str("old_state", oldState.GoString()).
			Str("new_state", newState.GoString()).
			Msg("channel_state_transition")
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

func (self *Channel) getState() state {
	return state(atomic.LoadInt32(&self.state))
}

func (self *Channel) getStream() *stream.Stream {
	return (*stream.Stream)(atomic.LoadPointer(&self.stream))
}

func (self *Channel) getNextSequenceNumber() int {
	return int((atomic.AddUint32(&self.nextSequenceNumber, 1) - 1) & 0x7FFFFFFF)
}

func (self *Channel) isClosed() bool {
	state_ := self.getState()
	return !(state_ >= initial && state_ < closed)
}

var (
	ErrHandshakeRefused = errors.New("pbrpc/channel: handshake refused")
	ErrBroken           = errors.New("pbrpc/channel: broken")
	ErrClosed           = errors.New("pbrpc/channel: closed")
)

const (
	initial = 1 + iota
	establishing
	established
	closed
)

type state int

func (self state) GoString() string {
	switch self {
	case initial:
		return "<initial>"
	case establishing:
		return "<establishing>"
	case established:
		return "<established>"
	case closed:
		return "<closed>"
	default:
		return fmt.Sprintf("<state:%d>", self)
	}
}
