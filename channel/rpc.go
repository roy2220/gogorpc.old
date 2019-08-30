package channel

import (
	"context"

	"github.com/let-z-go/toolkit/uuid"
)

type RPC struct {
	Ctx               context.Context
	ServiceName       string
	MethodName        string
	RequestExtraData  ExtraData
	Request           Message
	ResponseExtraData ExtraData
	Response          Message
	Err               error

	internals rpcInternals
}

func (self *RPC) Handle() bool {
	return self.internals.Handle(self)
}

func (self *RPC) GetTraceID() uuid.UUID {
	return self.internals.TraceID
}

type ExtraData = map[string][]byte

type RPCHandler func(rpc *RPC)

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

type rpcKey struct{}
