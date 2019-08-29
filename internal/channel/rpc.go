package channel

import (
	"context"

	"github.com/let-z-go/toolkit/uuid"

	"github.com/let-z-go/pbrpc/internal/stream"
)

type RPC struct {
	Ctx             context.Context
	TraceID         uuid.UUID
	ServiceName     string
	MethodName      string
	InputExtraData  ExtraData
	Request         stream.Message
	OutputExtraData ExtraData
	Response        stream.Message
	Err             error

	internals RPCInternals
}

func (self *RPC) Handle() bool {
	return self.internals.Handle(self)
}

type ExtraData = map[string][]byte

type RPCInternals struct {
	SequenceNumber int32
	Deadline       int64

	handler              RPCHandler
	interceptors         []RPCHandler
	nextInterceptorIndex int
}

func (self *RPCInternals) Init(handler RPCHandler, interceptors []RPCHandler) *RPCInternals {
	self.handler = handler
	self.interceptors = interceptors
	return self
}

func (self *RPCInternals) Handle(externals *RPC) bool {
	if i, n := self.nextInterceptorIndex, len(self.interceptors); i <= n {
		self.nextInterceptorIndex++

		if i == n {
			self.handler(externals)
		} else {
			self.interceptors[i](externals)
		}

		return true
	}

	return false
}

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

type rpcKey struct{}
