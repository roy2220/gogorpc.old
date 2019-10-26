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

	internals rpcInternals
}

func (r *RPC) Handle() bool {
	return r.internals.Handle(r)
}

func (r *RPC) Reprepare() {
	r.ResponseExtraData = ExtraDataRef{}
	r.Response = nil
	r.Err = nil
	r.internals.Reprepare()
}

func (r *RPC) Channel() RestrictedChannel {
	return RestrictedChannel{r.internals.Channel}
}

func (r *RPC) TraceID() uuid.UUID {
	return r.internals.TraceID
}

func (r *RPC) IsHandled() bool {
	return r.internals.IsHandled()
}

type RestrictedChannel struct {
	underlying *Channel
}

func (rc RestrictedChannel) DoRPC(rpc *RPC, responseFactory MessageFactory) {
	rc.underlying.DoRPC(rpc, responseFactory)
}

func (rc RestrictedChannel) PrepareRPC(rpc *RPC, responseFactory MessageFactory) {
	rc.underlying.PrepareRPC(rpc, responseFactory)
}

func (rc RestrictedChannel) Abort(extraData ExtraData) {
	rc.underlying.Abort(extraData)
}

func (rc RestrictedChannel) IsServerSide() bool {
	return rc.underlying.IsServerSide()
}

func (rc RestrictedChannel) TransportID() uuid.UUID {
	return rc.underlying.TransportID()
}

func (rc RestrictedChannel) UserData() interface{} {
	return rc.underlying.UserData()
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
	Channel        *Channel
	SequenceNumber int32
	Deadline       int64
	TraceID        uuid.UUID

	handler              RPCHandler
	interceptors         []RPCHandler
	nextInterceptorIndex int
}

func (ri *rpcInternals) Init(handler RPCHandler, interceptors []RPCHandler) {
	ri.handler = handler
	ri.interceptors = interceptors
}

func (ri *rpcInternals) Handle(externals *RPC) bool {
	if i, n := ri.nextInterceptorIndex, len(ri.interceptors); i <= n {
		ri.nextInterceptorIndex++

		if i < n {
			ri.interceptors[i](externals)
		} else {
			ri.handler(externals)
		}

		return true
	}

	return false
}

func (ri *rpcInternals) Reprepare() {
	ri.nextInterceptorIndex = 0
}

func (ri *rpcInternals) IsHandled() bool {
	return ri.nextInterceptorIndex >= 1
}

type rpcKey struct{}
