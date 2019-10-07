package channel

import (
	"context"
	"errors"
	"time"

	"github.com/let-z-go/gogorpc/internal/protocol"
	"github.com/let-z-go/gogorpc/internal/stream"
	"github.com/let-z-go/toolkit/uuid"
)

var ErrDirectResponse = errors.New("gogorpc/channel: direct response")

type messageProcessor struct {
	Channel *Channel
	Stream  *stream.Stream

	methodOptionsCache *MethodOptions
	inflightRPCCache   *inflightRPC
}

var _ = stream.MessageProcessor((*messageProcessor)(nil))

func (self *messageProcessor) NewKeepalive(packet *Packet) {
	packet.Message = self.Channel.extension.NewKeepalive()
}

func (self *messageProcessor) HandleKeepalive(ctx context.Context, packet *Packet) {
	packet.Err = self.Channel.extension.HandleKeepalive(ctx, packet.Message)
}

func (self *messageProcessor) EmitKeepalive(packet *Packet) {
	packet.Message, packet.Err = self.Channel.extension.EmitKeepalive()
}

func (self *messageProcessor) NewRequest(packet *Packet) {
	methodOptions := self.Channel.options.GetMethod(packet.RequestHeader.ServiceId, packet.RequestHeader.MethodName)
	packet.Message = methodOptions.RequestFactory()
	self.methodOptionsCache = methodOptions
}

func (self *messageProcessor) HandleRequest(ctx context.Context, packet *Packet) {
	requestHeader := &packet.RequestHeader
	traceID := uuid.UUID{requestHeader.TraceId.Low, requestHeader.TraceId.High}

	if packet.Err != nil {
		if packet.Err == ErrDirectResponse {
			// direct response by packet filter
			responseHeader := &packet.ResponseHeader
			responseHeader.SequenceNumber = requestHeader.SequenceNumber

			if packet.ResponseHeader.ErrorType == 0 {
				self.Stream.SendResponse(responseHeader, packet.Message)
			} else {
				self.Stream.SendResponse(responseHeader, NullMessage)
			}
		} else {
			self.Channel.options.Logger.Info().Err(packet.Err).
				Str("transport_id", self.Stream.TransportID().String()).
				Str("trace_id", traceID.String()).
				Str("service_id", requestHeader.ServiceId).
				Str("method_name", requestHeader.MethodName).
				Msg("rpc_bad_request")

			self.Stream.SendResponse(&protocol.ResponseHeader{
				SequenceNumber: requestHeader.SequenceNumber,
				ErrorType:      RPCErrBadRequest.Type,
				ErrorCode:      RPCErrBadRequest.Code,
			}, NullMessage)
		}

		packet.Err = nil
		return
	}

	if self.methodOptionsCache.IncomingRPCHandler == nil {
		self.Channel.options.Logger.Info().
			Str("transport_id", self.Stream.TransportID().String()).
			Str("trace_id", traceID.String()).
			Str("service_id", requestHeader.ServiceId).
			Str("method_name", requestHeader.MethodName).
			Msg("rpc_not_found")

		self.Stream.SendResponse(&protocol.ResponseHeader{
			SequenceNumber: requestHeader.SequenceNumber,
			ErrorType:      RPCErrNotFound.Type,
			ErrorCode:      RPCErrNotFound.Code,
		}, NullMessage)

		return
	}

	rpc := GetPooledRPC()

	*rpc = RPC{
		Ctx:              ctx,
		ServiceID:        requestHeader.ServiceId,
		MethodName:       requestHeader.MethodName,
		RequestExtraData: ExtraData(requestHeader.ExtraData).Ref(false),
		Request:          packet.Message,

		internals: rpcInternals{
			Channel:        self.Channel,
			SequenceNumber: requestHeader.SequenceNumber,
			Deadline:       requestHeader.Deadline,
			TraceID:        traceID,
		},
	}

	rpc.internals.Init(self.methodOptionsCache.IncomingRPCHandler, self.methodOptionsCache.IncomingRPCInterceptors)
	stream_ := self.Stream

	go func() {
		handleIncomingRPC(rpc, stream_)
	}()
}

func (self *messageProcessor) PostEmitRequest(packet *Packet) {
	requestHeader := &packet.RequestHeader
	value, _ := self.Channel.inflightRPCs.Load(requestHeader.SequenceNumber)
	inflightRPC_ := value.(*inflightRPC)

	if packet.Err == nil {
		inflightRPC_.IsEmitted = true
	} else {
		self.Channel.inflightRPCs.Delete(requestHeader.SequenceNumber)

		if packet.Err == ErrDirectResponse {
			// direct response by packet filter
			responseHeader := &packet.ResponseHeader

			if responseHeader.ErrorType == 0 {
				self.inflightRPCCache.Succeed(responseHeader.ExtraData, packet.Message)
			} else {
				inflightRPC_.Fail(responseHeader.ExtraData, &RPCError{
					Type: responseHeader.ErrorType,
					Code: responseHeader.ErrorCode,
				})
			}
		} else {
			inflightRPC_.Fail(nil, packet.Err)
		}

		packet.Err = ErrPacketDropped
	}
}

func (self *messageProcessor) NewResponse(packet *Packet) {
	responseHeader := &packet.ResponseHeader
	value, ok := self.Channel.inflightRPCs.Load(responseHeader.SequenceNumber)

	if !ok {
		self.Channel.options.Logger.Warn().
			Str("transport_id", self.Stream.TransportID().String()).
			Int("sequence_number", int(responseHeader.SequenceNumber)).
			Msg("channel_ignored_response")
		packet.Err = ErrPacketDropped
		return
	}

	inflightRPC_ := value.(*inflightRPC)

	if !inflightRPC_.IsEmitted {
		self.Channel.options.Logger.Warn().
			Str("transport_id", self.Stream.TransportID().String()).
			Int("sequence_number", int(responseHeader.SequenceNumber)).
			Msg("channel_ignored_response")
		packet.Err = ErrPacketDropped
		return
	}

	self.Channel.inflightRPCs.Delete(responseHeader.SequenceNumber)

	if responseHeader.ErrorType == 0 {
		packet.Message = inflightRPC_.NewResponse()
	} else {
		packet.Message = NullMessage
	}

	self.inflightRPCCache = inflightRPC_
}

func (self *messageProcessor) HandleResponse(ctx context.Context, packet *Packet) {
	responseHeader := &packet.ResponseHeader

	if packet.Err == nil {
		if responseHeader.ErrorType == 0 {
			self.inflightRPCCache.Succeed(responseHeader.ExtraData, packet.Message)
		} else {
			self.inflightRPCCache.Fail(responseHeader.ExtraData, &RPCError{
				Type: responseHeader.ErrorType,
				Code: responseHeader.ErrorCode,
			})
		}
	} else {
		self.inflightRPCCache.Fail(responseHeader.ExtraData, packet.Err)
		packet.Err = nil
	}
}

func (self *messageProcessor) PostEmitResponse(packet *Packet) {
}

func handleIncomingRPC(rpc *RPC, stream_ *stream.Stream) {
	var cancel context.CancelFunc

	if rpc.internals.Deadline == 0 {
		rpc.Ctx, cancel = context.WithCancel(rpc.Ctx)
	} else {
		rpc.Ctx, cancel = context.WithDeadline(rpc.Ctx, time.Unix(0, rpc.internals.Deadline))
	}

	defer cancel()
	rpc.Ctx = BindRPC(rpc.Ctx, rpc)
	rpc.Handle()

	responseHeader := protocol.ResponseHeader{
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
				Str("service_id", rpc.ServiceID).
				Str("method_name", rpc.MethodName).
				Msg("rpc_internal_server_error")
			rpcErr = RPCErrInternalServer
		}

		responseHeader.ErrorType = rpcErr.Type
		responseHeader.ErrorCode = rpcErr.Code
		response = NullMessage
	}

	PutPooledRPC(rpc)
	stream_.SendResponse(&responseHeader, response)
}
