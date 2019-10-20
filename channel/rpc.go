package channel

import (
	"context"

	"github.com/let-z-go/toolkit/uuid"
)

type RPC struct {
	Ctx              context.Context
	ServiceName      string
	MethodName       string
	RequestExtraData ExtraDataRef
	Request          Message

	ResponseExtraData ExtraDataRef
	Response          Message
	Err               error

	channel        *Channel
	sequenceNumber int32
	deadline       int64
	traceID        uuid.UUID

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

func (self *RPC) Channel() RestrictedChannel {
	return RestrictedChannel{self.channel}
}

func (self *RPC) TraceID() uuid.UUID {
	return self.traceID
}

func (self *RPC) IsHandled() bool {
	return self.internals.IsHandled()
}

type RestrictedChannel struct {
	underlying *Channel
}

func (self RestrictedChannel) DoRPC(rpc *RPC, responseFactory MessageFactory) {
	self.underlying.DoRPC(rpc, responseFactory)
}

func (self RestrictedChannel) PrepareRPC(rpc *RPC, responseFactory MessageFactory) {
	self.underlying.PrepareRPC(rpc, responseFactory)
}

func (self RestrictedChannel) Abort(extraData ExtraData) {
	self.underlying.Abort(extraData)
}

func (self RestrictedChannel) IsServerSide() bool {
	return self.underlying.IsServerSide()
}

func (self RestrictedChannel) UserData() interface{} {
	return self.underlying.UserData()
}

func (self RestrictedChannel) TransportID() uuid.UUID {
	return self.underlying.TransportID()
}

type RPCHandler func(rpc *RPC)

type RPCPreparer interface {
	PrepareRPC(rpc *RPC, responseFactory MessageFactory)
}

func BindRPC(ctx context.Context, rpc *RPC) context.Context {
	return context.WithValue(ctx, rpcKey{}, rpc)
}

func GetRPC(ctx context.Context) (*RPC, bool) {
	if value := ctx.Value(rpcKey{}); value != nil {
		return value.(*RPC), true
	}

	return nil, false
}

func MustGetRPC(ctx context.Context) *RPC {
	return ctx.Value(rpcKey{}).(*RPC)
}

type rpcInternals struct {
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
