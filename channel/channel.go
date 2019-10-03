package channel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/let-z-go/intrusives/list"
	"github.com/let-z-go/toolkit/deque"
	"github.com/let-z-go/toolkit/uuid"

	"github.com/let-z-go/gogorpc/internal/protocol"
	"github.com/let-z-go/gogorpc/internal/stream"
)

type Channel struct {
	options                *Options
	dequeOfPendingRequests deque.Deque
	stream_                unsafe.Pointer
	state_                 int32
	nextSequenceNumber     uint32
	inflightRPCs           sync.Map
	pendingAbort           atomic.Value
	extension              Extension
}

func (self *Channel) Init(isServerSide bool, options *Options) *Channel {
	self.options = options.Normalize()
	self.dequeOfPendingRequests.Init(0)

	self.stream_ = unsafe.Pointer(new(stream.Stream).Init(
		isServerSide,
		self.options.Stream,
		uuid.UUID{},
		&self.dequeOfPendingRequests,
		nil,
	))

	self.state_ = int32(initial)
	self.extension = self.options.ExtensionFactory(isServerSide)
	return self
}

func (self *Channel) Close() {
	self.setState(closed)
	self.extension.OnClosed(self)
}

func (self *Channel) Establish(ctx context.Context, serverURL *url.URL, connection net.Conn) error {
	if self.state() == initial {
		self.setState(establishing)
		self.extension.OnEstablishing(self, serverURL)
	} else {
		self.setState(reestablishing)
		self.extension.OnReestablishing(self, serverURL)
	}

	ok, err := self.stream().Establish(ctx, connection, self.extension)

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
	self.extension.OnEstablished(self)
	stream_ := self.stream()

	err := stream_.Process(ctx, &messageProcessor{
		Keepaliver:   self.extension,
		Options:      self.options,
		Stream:       stream_,
		InflightRPCs: &self.inflightRPCs,
	}, self.extension)

	self.extension.OnBroken(self, err)
	return err
}

func (self *Channel) InvokeRPC(rpc *RPC, responseFactory MessageFactory) {
	self.PrepareRPC(rpc, responseFactory)
	rpc.Handle()
}

func (self *Channel) PrepareRPC(rpc *RPC, responseFactory MessageFactory) {
	if parentRPC, ok := GetRPC(rpc.Ctx); ok {
		rpc.internals.TraceID = parentRPC.internals.TraceID
	} else {
		rpc.internals.TraceID = uuid.GenerateUUID4Fast()
	}

	var rpcHandler RPCHandler

	if self.isClosed() {
		rpcHandler = func(rpc *RPC) {
			rpc.Err = ErrClosed
		}
	} else {
		rpc.internals.SequenceNumber = int32(self.getNextSequenceNumber())

		if deadline, ok := rpc.Ctx.Deadline(); ok {
			rpc.internals.Deadline = deadline.UnixNano()
		} else {
			rpc.internals.Deadline = 0
		}

		rpcHandler = func(rpc *RPC) {
			pendingRPC_ := newPooledPendingRPC()
			pendingRPC_.Init(responseFactory)
			self.inflightRPCs.Store(rpc.internals.SequenceNumber, pendingRPC_)

			if err := self.stream().SendRequest(rpc.Ctx, &protocol.RequestHeader{
				SequenceNumber: rpc.internals.SequenceNumber,
				ServiceId:      rpc.ServiceID,
				MethodName:     rpc.MethodName,
				Metadata:       rpc.RequestMetadata,
				Deadline:       rpc.internals.Deadline,

				TraceId: protocol.UUID{
					Low:  rpc.internals.TraceID[0],
					High: rpc.internals.TraceID[1],
				},
			}, rpc.Request); err != nil {
				self.inflightRPCs.Delete(rpc.internals.SequenceNumber)

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

			rpc.ResponseMetadata = pendingRPC_.ResponseMetadata
			rpc.Response = pendingRPC_.Response
			rpc.Err = pendingRPC_.Err
			putPooledPendingRPC(pendingRPC_)
		}
	}

	methodOptions := self.options.GetMethod(rpc.ServiceID, rpc.MethodName)
	rpc.internals.Init(rpcHandler, methodOptions.OutgoingRPCInterceptors)
	rpc.Ctx = BindRPC(rpc.Ctx, rpc)
}

func (self *Channel) Abort(metadata Metadata) {
	self.pendingAbort.Store(metadata)
	self.stream().Abort(metadata)
}

func (self *Channel) TransportID() uuid.UUID {
	return self.stream().TransportID()
}

func (self *Channel) setState(newState state) {
	oldState := self.state()

	switch oldState {
	case initial:
		switch newState {
		case establishing, closed:
			goto ValidStateTransition
		}
	case establishing, reestablishing:
		switch newState {
		case reestablishing, established, closed:
			goto ValidStateTransition
		}
	case established:
		switch newState {
		case reestablishing, closed:
			goto ValidStateTransition
		}
	}

	panic(fmt.Errorf("gogorpc/channel: invalid state transition: oldState=%#v, newState=%#v", oldState, newState))

ValidStateTransition:
	if newState != oldState {
		atomic.StoreInt32(&self.state_, int32(newState))
		logEvent := self.options.Logger.Info()

		if oldState != initial {
			logEvent.Str("transport_id", self.TransportID().String())
		}

		logEvent.Str("old_state", oldState.GoString()).
			Str("new_state", newState.GoString()).
			Msg("channel_state_transition")
	}

	switch newState {
	case reestablishing:
		oldStream := self.stream()

		newStream := new(stream.Stream).Init(
			oldStream.IsServerSide(),
			self.options.Stream,
			oldStream.TransportID(),
			&self.dequeOfPendingRequests,
			nil,
		)

		if value := self.pendingAbort.Load(); value != nil {
			newStream.Abort(value.(Metadata))
		}

		atomic.StorePointer(&self.stream_, unsafe.Pointer(newStream))
		oldStream.Close()

		if oldState == established {
			self.inflightRPCs.Range(func(key interface{}, value interface{}) bool {
				pendingRPC_ := value.(*pendingRPC)

				if pendingRPC_.IsEmitted {
					self.inflightRPCs.Delete(key)
					pendingRPC_.Fail(nil, ErrBroken)
				}

				return true
			})
		}
	case closed:
		self.stream().Close()
		listOfPendingRequests := new(list.List).Init()
		self.dequeOfPendingRequests.Close(listOfPendingRequests)
		stream.PutPooledPendingRequests(listOfPendingRequests)

		self.inflightRPCs.Range(func(key interface{}, value interface{}) bool {
			self.inflightRPCs.Delete(key)
			pendingRPC_ := value.(*pendingRPC)

			if pendingRPC_.IsEmitted {
				pendingRPC_.Fail(nil, ErrBroken)
			} else {
				pendingRPC_.Fail(nil, ErrClosed)
			}

			return true
		})
	}
}

func (self *Channel) getNextSequenceNumber() int {
	return int((atomic.AddUint32(&self.nextSequenceNumber, 1) - 1) & 0x7FFFFFFF)
}

func (self *Channel) isClosed() bool {
	state_ := self.state()
	return !(state_ >= initial && state_ < closed)
}

func (self *Channel) stream() *stream.Stream {
	return (*stream.Stream)(atomic.LoadPointer(&self.stream_))
}

func (self *Channel) state() state {
	return state(atomic.LoadInt32(&self.state_))
}

var (
	ErrHandshakeRefused = errors.New("gogorpc/channel: handshake refused")
	ErrBroken           = errors.New("gogorpc/channel: broken")
	ErrClosed           = errors.New("gogorpc/channel: closed")
)

const (
	initial = 1 + iota
	establishing
	established
	reestablishing
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
	case reestablishing:
		return "<reestablishing>"
	case closed:
		return "<closed>"
	default:
		return fmt.Sprintf("<state:%d>", self)
	}
}
