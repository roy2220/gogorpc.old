package channel

import (
	"context"

	"github.com/let-z-go/toolkit/uuid"
)

type RPC struct {
	Ctx              context.Context
	ServiceID        string
	MethodName       string
	RequestExtraData ExtraDataRef
	Request          Message

	ResponseExtraData ExtraDataRef
	Response          Message
	Err               error

	internals rpcInternals
}

func (self *RPC) Handle() bool {
	return self.internals.Handle(self)
}

func (self *RPC) Reprepare() {
	self.ResponseExtraData = ExtraDataRef{}
	self.Response = nil
	self.Err = nil
	self.internals.Reprepare()
}

func (self *RPC) IsHandled() bool {
	return self.internals.IsHandled()
}

func (self *RPC) Channel() interface {
	DoRPC(rpc *RPC, responseFactory MessageFactory)
	PrepareRPC(rpc *RPC, responseFactory MessageFactory)
	Abort(extraData ExtraData)
	TransportID() uuid.UUID
	Extension() Extension
} {
	return self.internals.Channel
}

func (self *RPC) TraceID() uuid.UUID {
	return self.internals.TraceID
}

type RPCHandler func(rpc *RPC)

type RPCPreparer interface {
	PrepareRPC(rpc *RPC, responseFactory MessageFactory)
}

func BindRPC(ctx context.Context, rpc *RPC) context.Context {
	return context.WithValue(ctx, rpcKey{}, rpc)
}

func GetRPC(ctx context.Context) (*RPC, bool) {
	value := ctx.Value(rpcKey{})

	if value == nil {
		return nil, false
	}

	return value.(*RPC), true
}

func MustGetRPC(ctx context.Context) *RPC {
	return ctx.Value(rpcKey{}).(*RPC)
}

type rpcInternals struct {
	Channel        *Channel
	SequenceNumber int32
	Deadline       int64
	TraceID        uuid.UUID

	handler              RPCHandler
	interceptors         []RPCHandler
	nextInterceptorIndex int
}

func (self *rpcInternals) Init(handler RPCHandler, interceptors []RPCHandler) {
	self.handler = handler
	self.interceptors = interceptors
}

func (self *rpcInternals) Handle(externals *RPC) bool {
	if i, n := self.nextInterceptorIndex, len(self.interceptors); i <= n {
		self.nextInterceptorIndex++

		if i < n {
			self.interceptors[i](externals)
		} else {
			self.handler(externals)
		}

		return true
	}

	return false
}

func (self *rpcInternals) Reprepare() {
	self.nextInterceptorIndex = 0
}

func (self *rpcInternals) IsHandled() bool {
	return self.nextInterceptorIndex >= 1
}

type rpcKey struct{}
