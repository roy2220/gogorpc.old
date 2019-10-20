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

func (self *messageProcessor) NewKeepalive(event *Event) {
	event.Message = self.Keepaliver.NewKeepalive()
}

func (self *messageProcessor) HandleKeepalive(ctx context.Context, event *Event) {
	event.Err = self.Keepaliver.HandleKeepalive(ctx, event.Message)
}

func (self *messageProcessor) EmitKeepalive(event *Event) {
	event.Message, event.Err = self.Keepaliver.EmitKeepalive()
}

func (self *messageProcessor) NewRequest(event *Event) {
	methodOptions := self.Channel.options.GetMethod(event.RequestHeader.ServiceName, event.RequestHeader.MethodName)
	event.Message = methodOptions.RequestFactory()
	self.methodOptionsCache = methodOptions
}

func (self *messageProcessor) HandleRequest(ctx context.Context, event *Event) {
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
			self.Channel.options.Logger.Info().Err(event.Err).
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

	if self.methodOptionsCache.IncomingRPCHandler == nil {
		self.Channel.options.Logger.Info().
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

		channel:        self.Channel,
		sequenceNumber: requestHeader.SequenceNumber,
		deadline:       requestHeader.Deadline,
		traceID:        traceID,
	}

	rpc.internals.Init(self.methodOptionsCache.IncomingRPCHandler, self.methodOptionsCache.IncomingRPCInterceptors)
	go handleIncomingRPC(rpc, event.Stream())
}

func (self *messageProcessor) PostEmitRequest(event *Event) {
	requestHeader := &event.RequestHeader
	value, _ := self.Channel.inflightRPCs.Load(requestHeader.SequenceNumber)
	inflightRPC_ := value.(*inflightRPC)

	if event.Err == nil {
		inflightRPC_.IsEmitted = true
	} else {
		self.Channel.inflightRPCs.Delete(requestHeader.SequenceNumber)

		if event.Err == ErrDirectResponse {
			// direct response by event filter
			responseHeader := &event.ResponseHeader

			if responseHeader.RpcError.Type == 0 {
				self.inflightRPCCache.Succeed(responseHeader.ExtraData, event.Message)
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

func (self *messageProcessor) NewResponse(event *Event) {
	responseHeader := &event.ResponseHeader
	value, ok := self.Channel.inflightRPCs.Load(responseHeader.SequenceNumber)

	if !ok {
		self.Channel.options.Logger.Warn().
			Str("transport_id", event.Stream().TransportID().String()).
			Int("sequence_number", int(responseHeader.SequenceNumber)).
			Msg("channel_ignored_response")
		event.Err = ErrEventDropped
		return
	}

	inflightRPC_ := value.(*inflightRPC)

	if !inflightRPC_.IsEmitted {
		self.Channel.options.Logger.Warn().
			Str("transport_id", event.Stream().TransportID().String()).
			Int("sequence_number", int(responseHeader.SequenceNumber)).
			Msg("channel_ignored_response")
		event.Err = ErrEventDropped
		return
	}

	self.Channel.inflightRPCs.Delete(responseHeader.SequenceNumber)

	if responseHeader.RpcError.Type == 0 {
		event.Message = inflightRPC_.NewResponse()
	} else {
		event.Message = NullMessage
	}

	self.inflightRPCCache = inflightRPC_
}

func (self *messageProcessor) HandleResponse(ctx context.Context, event *Event) {
	responseHeader := &event.ResponseHeader

	if event.Err == nil {
		if responseHeader.RpcError.Type == 0 {
			self.inflightRPCCache.Succeed(responseHeader.ExtraData, event.Message)
		} else {
			rpcErr := (RPCError)(responseHeader.RpcError)
			self.inflightRPCCache.Fail(responseHeader.ExtraData, &rpcErr)
		}
	} else {
		self.inflightRPCCache.Fail(responseHeader.ExtraData, event.Err)
		event.Err = nil
	}
}

func (self *messageProcessor) PostEmitResponse(event *Event) {
}

func handleIncomingRPC(rpc *RPC, stream_ stream.RestrictedStream) {
	var cancel context.CancelFunc

	if rpc.deadline == 0 {
		rpc.Ctx, cancel = context.WithCancel(rpc.Ctx)
	} else {
		rpc.Ctx, cancel = context.WithDeadline(rpc.Ctx, time.Unix(0, rpc.deadline))
	}

	defer cancel()
	rpc.Ctx = BindRPC(rpc.Ctx, rpc)
	rpc.Handle()

	responseHeader := proto.ResponseHeader{
		SequenceNumber: rpc.sequenceNumber,
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
			rpc.channel.options.Logger.Error().Err(rpc.Err).
				Str("transport_id", stream_.TransportID().String()).
				Str("trace_id", rpc.traceID.String()).
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
