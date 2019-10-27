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

	"github.com/let-z-go/toolkit/deque"
	"github.com/let-z-go/toolkit/uuid"

	"github.com/let-z-go/gogorpc/internal/proto"
	"github.com/let-z-go/gogorpc/internal/stream"
)

type Channel struct {
	options                *Options
	extension              Extension
	dequeOfPendingRequests deque.Deque
	stream_                unsafe.Pointer
	pendingAbort           atomic.Value
	state_                 int32
	nextSequenceNumber     uint32
	inflightRPCs           sync.Map
}

func (c *Channel) Init(options *Options, isServerSide bool) *Channel {
	c.options = options.Normalize()
	c.extension = c.options.ExtensionFactory(RestrictedChannel{c}, isServerSide)
	c.dequeOfPendingRequests.Init(0)

	c.stream_ = unsafe.Pointer(new(stream.Stream).Init(
		c.options.Stream,
		isServerSide,
		uuid.UUID{},
		c.extension.NewUserData(),
		&c.dequeOfPendingRequests,
	))

	c.state_ = int32(initial)
	c.extension.OnInitialized()
	return c
}

func (c *Channel) Close() {
	c.setState(closed)
	c.extension.OnClosed()
}

func (c *Channel) Run(ctx context.Context, serverURL *url.URL, connection net.Conn) error {
	if c.state() == initial {
		c.setState(establishing)
		c.extension.OnEstablishing(serverURL)
	} else {
		c.setState(reestablishing)
		c.extension.OnReestablishing(serverURL)
	}

	ok, err := c.stream().Establish(ctx, connection, c.extension.NewHandshaker())

	if err != nil {
		return err
	}

	if !ok {
		return ErrHandshakeRefused
	}

	c.setState(established)
	c.extension.OnEstablished()
	stream_ := c.stream()

	err = stream_.Process(ctx, c.extension.NewTrafficCrypter(), &messageProcessor{
		Channel:    c,
		Keepaliver: c.extension.NewKeepaliver(),
	})

	c.extension.OnBroken(err)
	return err
}

func (c *Channel) DoRPC(rpc *RPC, responseFactory MessageFactory) {
	c.PrepareRPC(rpc, responseFactory)
	rpc.Handle()
}

func (c *Channel) PrepareRPC(rpc *RPC, responseFactory MessageFactory) {
	rpc.internals.Channel = c
	rpcParent, rpcHasParent := GetRPC(rpc.Ctx)

	if rpcHasParent {
		rpc.internals.TraceID = rpcParent.internals.TraceID

		for key, value := range rpcParent.RequestExtraData.Value() {
			if !(len(key) >= 1 && key[0] == '_') {
				continue
			}

			if _, ok := rpc.RequestExtraData.TryGet(key); ok {
				continue
			}

			rpc.RequestExtraData.Set(key, value)
		}
	} else {
		rpc.internals.TraceID = uuid.GenerateUUID4Fast()
	}

	var rpcHandler RPCHandler

	if c.isClosed() {
		rpcHandler = func(rpc *RPC) {
			rpc.Err = ErrClosed
		}
	} else {
		rpc.internals.SequenceNumber = int32(c.getNextSequenceNumber())

		if deadline, ok := rpc.Ctx.Deadline(); ok {
			rpc.internals.Deadline = deadline.UnixNano()
		} else {
			rpc.internals.Deadline = 0
		}

		rpcHandler = func(rpc *RPC) {
			handleOutgoingRPC(rpc, responseFactory)

			if rpcHasParent {
				for key, value := range rpc.ResponseExtraData.Value() {
					if !(len(key) >= 1 && key[0] == '_') {
						continue
					}

					rpcParent.ResponseExtraData.Set(key, value)
				}
			}
		}
	}

	methodOptions := c.options.GetMethod(rpc.ServiceName, rpc.MethodName)
	rpc.internals.Init(rpcHandler, methodOptions.OutgoingRPCInterceptors)
	rpc.Ctx = BindRPC(rpc.Ctx, rpc)
}

func (c *Channel) Abort(extraData ExtraData) {
	c.pendingAbort.Store(extraData)
	c.stream().Abort(extraData)
}

func (c *Channel) IsServerSide() bool {
	return c.stream().IsServerSide()
}

func (c *Channel) TransportID() uuid.UUID {
	return c.stream().TransportID()
}

func (c *Channel) UserData() interface{} {
	return c.stream().UserData()
}

func (c *Channel) setState(newState state) {
	oldState := c.state()

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
		atomic.StoreInt32(&c.state_, int32(newState))
		logEvent := c.options.Logger.Info()

		if oldState != initial {
			logEvent.Str("transport_id", c.TransportID().String())
		}

		logEvent.Str("old_state", oldState.GoString()).
			Str("new_state", newState.GoString()).
			Msg("channel_state_transition")
	}

	switch newState {
	case reestablishing:
		oldStream := c.stream()

		newStream := new(stream.Stream).Init(
			c.options.Stream,
			oldStream.IsServerSide(),
			oldStream.TransportID(),
			c.extension.NewUserData(),
			&c.dequeOfPendingRequests,
		)

		if value := c.pendingAbort.Load(); value != nil {
			newStream.Abort(value.(ExtraData))
		}

		atomic.StorePointer(&c.stream_, unsafe.Pointer(newStream))
		oldStream.Close()

		if oldState == established {
			c.inflightRPCs.Range(func(key interface{}, value interface{}) bool {
				inflightRPC_ := value.(*inflightRPC)

				if inflightRPC_.IsEmitted {
					c.inflightRPCs.Delete(key)
					inflightRPC_.Fail(nil, ErrBroken)
				}

				return true
			})
		}
	case closed:
		c.stream().Close()
		listOfPendingRequests := deque.NewList()
		c.dequeOfPendingRequests.Close(listOfPendingRequests)
		stream.PutPooledPendingRequests(listOfPendingRequests)

		c.inflightRPCs.Range(func(key interface{}, value interface{}) bool {
			c.inflightRPCs.Delete(key)
			inflightRPC_ := value.(*inflightRPC)

			if inflightRPC_.IsEmitted {
				inflightRPC_.Fail(nil, ErrBroken)
			} else {
				inflightRPC_.Fail(nil, ErrClosed)
			}

			return true
		})
	}
}

func (c *Channel) getNextSequenceNumber() int {
	return int((atomic.AddUint32(&c.nextSequenceNumber, 1) - 1) & 0x7FFFFFFF)
}

func (c *Channel) isClosed() bool {
	state_ := c.state()
	return !(state_ >= initial && state_ < closed)
}

func (c *Channel) stream() *stream.Stream {
	return (*stream.Stream)(atomic.LoadPointer(&c.stream_))
}

func (c *Channel) state() state {
	return state(atomic.LoadInt32(&c.state_))
}

var (
	ErrHandshakeRefused = errors.New("gogorpc/channel: handshake refused")
	ErrBroken           = errors.New("gogorpc/channel: broken")
	ErrClosed           = errors.New("gogorpc/channel: closed")
)

const (
	initial = state(1 + iota)
	establishing
	established
	reestablishing
	closed
)

type state int

func (s state) GoString() string {
	switch s {
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
		return fmt.Sprintf("<state:%d>", s)
	}
}

func handleOutgoingRPC(rpc *RPC, responseFactory MessageFactory) {
	channel := rpc.internals.Channel
	inflightRPC_ := getPooledInflightRPC(responseFactory)
	channel.inflightRPCs.Store(rpc.internals.SequenceNumber, inflightRPC_)

	if err := channel.stream().SendRequest(rpc.Ctx, &proto.RequestHeader{
		SequenceNumber: rpc.internals.SequenceNumber,
		ServiceName:    rpc.ServiceName,
		MethodName:     rpc.MethodName,
		ExtraData:      rpc.RequestExtraData.Value(),
		Deadline:       rpc.internals.Deadline,

		TraceId: proto.UUID{
			Low:  rpc.internals.TraceID[0],
			High: rpc.internals.TraceID[1],
		},
	}, rpc.Request); err != nil {
		channel.inflightRPCs.Delete(rpc.internals.SequenceNumber)

		switch err {
		case stream.ErrClosed:
			rpc.Err = ErrClosed
		default:
			rpc.Err = err
		}

		return
	}

	if err := inflightRPC_.WaitFor(rpc.Ctx); err != nil {
		rpc.Err = err
		return
	}

	if inflightRPC_.Err == stream.ErrRequestExpired {
		<-rpc.Ctx.Done()
		inflightRPC_.Err = rpc.Ctx.Err()
	}

	rpc.ResponseExtraData = inflightRPC_.ResponseExtraData.Ref(false)
	rpc.Response = inflightRPC_.Response
	rpc.Err = inflightRPC_.Err
	putPooledInflightRPC(inflightRPC_)
}
