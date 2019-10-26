package channel

import (
	"context"
	"errors"
	"time"

	"github.com/let-z-go/gogorpc/internal/proto"
	"github.com/let-z-go/gogorpc/internal/stream"
	"github.com/let-z-go/toolkit/uuid"
)

var ErrDirectResponse = errors.New("gogorpc/channel: direct response")

type messageProcessor struct {
	Channel    *Channel
	Keepaliver Keepaliver

	methodOptionsCache *MethodOptions
	inflightRPCCache   *inflightRPC
}

var _ = stream.MessageProcessor((*messageProcessor)(nil))

func (mp *messageProcessor) NewKeepalive(event *Event) {
	event.Message = mp.Keepaliver.NewKeepalive()
}

func (mp *messageProcessor) HandleKeepalive(ctx context.Context, event *Event) {
	event.Err = mp.Keepaliver.HandleKeepalive(ctx, event.Message)
}

func (mp *messageProcessor) EmitKeepalive(event *Event) {
	event.Message, event.Err = mp.Keepaliver.EmitKeepalive()
}

func (mp *messageProcessor) NewRequest(event *Event) {
	methodOptions := mp.Channel.options.GetMethod(event.RequestHeader.ServiceName, event.RequestHeader.MethodName)
	event.Message = methodOptions.RequestFactory()
	mp.methodOptionsCache = methodOptions
}

func (mp *messageProcessor) HandleRequest(ctx context.Context, event *Event) {
	requestHeader := &event.RequestHeader
	traceID := uuid.UUID{requestHeader.TraceId.Low, requestHeader.TraceId.High}

	if event.Err != nil {
		if event.Err == ErrDirectResponse {
			// direct response by event filter
			responseHeader := &event.ResponseHeader
			responseHeader.SequenceNumber = requestHeader.SequenceNumber

			if event.ResponseHeader.RpcError.Type == 0 {
				event.Stream().SendResponse(responseHeader, event.Message)
			} else {
				event.Stream().SendResponse(responseHeader, NullMessage)
			}
		} else {
			mp.Channel.options.Logger.Info().Err(event.Err).
				Str("transport_id", event.Stream().TransportID().String()).
				Str("trace_id", traceID.String()).
				Str("service_name", requestHeader.ServiceName).
				Str("method_name", requestHeader.MethodName).
				Msg("rpc_bad_request")

			event.Stream().SendResponse(&proto.ResponseHeader{
				SequenceNumber: requestHeader.SequenceNumber,
				RpcError:       proto.RPCError(*RPCErrBadRequest),
			}, NullMessage)
		}

		event.Err = nil
		return
	}

	if mp.methodOptionsCache.IncomingRPCHandler == nil {
		mp.Channel.options.Logger.Info().
			Str("transport_id", event.Stream().TransportID().String()).
			Str("trace_id", traceID.String()).
			Str("service_name", requestHeader.ServiceName).
			Str("method_name", requestHeader.MethodName).
			Msg("rpc_not_found")

		event.Stream().SendResponse(&proto.ResponseHeader{
			SequenceNumber: requestHeader.SequenceNumber,
			RpcError:       proto.RPCError(*RPCErrNotFound),
		}, NullMessage)

		return
	}

	rpc := GetPooledRPC()

	*rpc = RPC{
		Ctx:              ctx,
		ServiceName:      requestHeader.ServiceName,
		MethodName:       requestHeader.MethodName,
		RequestExtraData: ExtraData(requestHeader.ExtraData).Ref(false),
		Request:          event.Message,

		internals: rpcInternals{
			Channel:        mp.Channel,
			SequenceNumber: requestHeader.SequenceNumber,
			Deadline:       requestHeader.Deadline,
			TraceID:        traceID,
		},
	}

	rpc.internals.Init(mp.methodOptionsCache.IncomingRPCHandler, mp.methodOptionsCache.IncomingRPCInterceptors)
	go handleIncomingRPC(rpc, event.Stream())
}

func (mp *messageProcessor) PostEmitRequest(event *Event) {
	requestHeader := &event.RequestHeader
	value, _ := mp.Channel.inflightRPCs.Load(requestHeader.SequenceNumber)
	inflightRPC_ := value.(*inflightRPC)

	if event.Err == nil {
		inflightRPC_.IsEmitted = true
	} else {
		mp.Channel.inflightRPCs.Delete(requestHeader.SequenceNumber)

		if event.Err == ErrDirectResponse {
			// direct response by event filter
			responseHeader := &event.ResponseHeader

			if responseHeader.RpcError.Type == 0 {
				mp.inflightRPCCache.Succeed(responseHeader.ExtraData, event.Message)
			} else {
				rpcErr := (RPCError)(responseHeader.RpcError)
				inflightRPC_.Fail(responseHeader.ExtraData, &rpcErr)
			}
		} else {
			inflightRPC_.Fail(nil, event.Err)
		}

		event.Err = ErrEventDropped
	}
}

func (mp *messageProcessor) NewResponse(event *Event) {
	responseHeader := &event.ResponseHeader
	value, ok := mp.Channel.inflightRPCs.Load(responseHeader.SequenceNumber)

	if !ok {
		mp.Channel.options.Logger.Warn().
			Str("transport_id", event.Stream().TransportID().String()).
			Int("sequence_number", int(responseHeader.SequenceNumber)).
			Msg("channel_ignored_response")
		event.Err = ErrEventDropped
		return
	}

	inflightRPC_ := value.(*inflightRPC)

	if !inflightRPC_.IsEmitted {
		mp.Channel.options.Logger.Warn().
			Str("transport_id", event.Stream().TransportID().String()).
			Int("sequence_number", int(responseHeader.SequenceNumber)).
			Msg("channel_ignored_response")
		event.Err = ErrEventDropped
		return
	}

	mp.Channel.inflightRPCs.Delete(responseHeader.SequenceNumber)

	if responseHeader.RpcError.Type == 0 {
		event.Message = inflightRPC_.NewResponse()
	} else {
		event.Message = NullMessage
	}

	mp.inflightRPCCache = inflightRPC_
}

func (mp *messageProcessor) HandleResponse(ctx context.Context, event *Event) {
	responseHeader := &event.ResponseHeader

	if event.Err == nil {
		if responseHeader.RpcError.Type == 0 {
			mp.inflightRPCCache.Succeed(responseHeader.ExtraData, event.Message)
		} else {
			rpcErr := (RPCError)(responseHeader.RpcError)
			mp.inflightRPCCache.Fail(responseHeader.ExtraData, &rpcErr)
		}
	} else {
		mp.inflightRPCCache.Fail(responseHeader.ExtraData, event.Err)
		event.Err = nil
	}
}

func (mp *messageProcessor) PostEmitResponse(event *Event) {
}

func handleIncomingRPC(rpc *RPC, stream_ stream.RestrictedStream) {
	var cancel context.CancelFunc

	if rpc.internals.Deadline == 0 {
		rpc.Ctx, cancel = context.WithCancel(rpc.Ctx)
	} else {
		rpc.Ctx, cancel = context.WithDeadline(rpc.Ctx, time.Unix(0, rpc.internals.Deadline))
	}

	defer cancel()
	rpc.Ctx = BindRPC(rpc.Ctx, rpc)
	rpc.Handle()

	responseHeader := proto.ResponseHeader{
		SequenceNumber: rpc.internals.SequenceNumber,
		ExtraData:      rpc.ResponseExtraData.Value(),
	}

	var response Message

	if rpc.Err == nil {
		response = rpc.Response
	} else {
		var rpcErr *RPCError

		if rpcErr2, ok := rpc.Err.(*RPCError); ok {
			rpcErr = rpcErr2
		} else {
			rpc.internals.Channel.options.Logger.Error().Err(rpc.Err).
				Str("transport_id", stream_.TransportID().String()).
				Str("trace_id", rpc.internals.TraceID.String()).
				Str("service_name", rpc.ServiceName).
				Str("method_name", rpc.MethodName).
				Msg("rpc_internal_server_error")
			rpcErr = RPCErrInternalServer
		}

		responseHeader.RpcError = proto.RPCError(*rpcErr)
		response = NullMessage
	}

	PutPooledRPC(rpc)
	stream_.SendResponse(&responseHeader, response)
}
