package channel

import (
	"context"
	"sync"
	"time"

	"github.com/let-z-go/gogorpc/internal/protocol"
	"github.com/let-z-go/gogorpc/internal/stream"
	"github.com/let-z-go/toolkit/uuid"
)

type messageProcessor struct {
	Options      *Options
	Stream       *stream.Stream
	InflightRPCs *sync.Map

	methodOptionsCache *MethodOptions
	pendingRPCCache    *pendingRPC
}

var _ = stream.MessageProcessor((*messageProcessor)(nil))

func (self *messageProcessor) NewKeepalive(packet *stream.Packet) {
	packet.Message = self.Options.Keepaliver.NewKeepalive()
}

func (self *messageProcessor) HandleKeepalive(ctx context.Context, packet *stream.Packet) {
	packet.Err = self.Options.Keepaliver.HandleKeepalive(ctx, packet.Message)
}

func (self *messageProcessor) EmitKeepalive(packet *stream.Packet) {
	packet.Message, packet.Err = self.Options.Keepaliver.EmitKeepalive()
}

func (self *messageProcessor) NewRequest(packet *stream.Packet) {
	methodOptions := self.Options.GetMethod(packet.RequestHeader.ServiceId, packet.RequestHeader.MethodName)
	packet.Message = methodOptions.RequestFactory()
	self.methodOptionsCache = methodOptions
}

func (self *messageProcessor) HandleRequest(ctx context.Context, packet *stream.Packet) {
	requestHeader := &packet.RequestHeader
	traceID := uuid.UUID{requestHeader.TraceId.Low, requestHeader.TraceId.High}

	if packet.Err != nil {
		self.Options.Logger.Info().Err(packet.Err).
			Str("transport_id", self.Stream.GetTransportID().String()).
			Str("trace_id", traceID.String()).
			Str("service_name", requestHeader.ServiceId).
			Str("method_name", requestHeader.MethodName).
			Msg("rpc_bad_request")
		packet.Err = nil

		self.Stream.SendResponse(&protocol.ResponseHeader{
			SequenceNumber: requestHeader.SequenceNumber,
			ErrorType:      RPCErrBadRequest.Type,
			ErrorCode:      RPCErrBadRequest.Code,
		}, NullMessage)

		return
	}

	if self.methodOptionsCache.IncomingRPCHandler == nil {
		self.Options.Logger.Info().
			Str("transport_id", self.Stream.GetTransportID().String()).
			Str("trace_id", traceID.String()).
			Str("service_name", requestHeader.ServiceId).
			Str("method_name", requestHeader.MethodName).
			Msg("rpc_not_found")

		self.Stream.SendResponse(&protocol.ResponseHeader{
			SequenceNumber: requestHeader.SequenceNumber,
			ErrorType:      RPCErrNotFound.Type,
			ErrorCode:      RPCErrNotFound.Code,
		}, NullMessage)

		return
	}

	rpc := RPC{
		Ctx:             ctx,
		ServiceID:       requestHeader.ServiceId,
		MethodName:      requestHeader.MethodName,
		RequestMetadata: requestHeader.Metadata,
		Request:         packet.Message,

		internals: rpcInternals{
			SequenceNumber: requestHeader.SequenceNumber,
			Deadline:       requestHeader.Deadline,
			TraceID:        traceID,
		},
	}

	rpc.internals.Init(self.methodOptionsCache.IncomingRPCHandler, self.methodOptionsCache.IncomingRPCInterceptors)

	go func() {
		var cancel context.CancelFunc

		if rpc.internals.Deadline == 0 {
			rpc.Ctx, cancel = context.WithCancel(rpc.Ctx)
		} else {
			rpc.Ctx, cancel = context.WithDeadline(rpc.Ctx, time.Unix(0, rpc.internals.Deadline))
		}

		defer cancel()
		rpc.Ctx = BindRPC(rpc.Ctx, &rpc)
		rpc.Handle()

		responseHeader := protocol.ResponseHeader{
			SequenceNumber: rpc.internals.SequenceNumber,
			Metadata:       rpc.ResponseMetadata,
		}

		var response Message

		if rpc.Err == nil {
			response = rpc.Response
		} else {
			if error_, ok := rpc.Err.(*RPCError); ok {
				responseHeader.ErrorType = error_.Type
				responseHeader.ErrorCode = error_.Code
			} else {
				self.Options.Logger.Error().Err(rpc.Err).
					Str("transport_id", self.Stream.GetTransportID().String()).
					Str("trace_id", rpc.internals.TraceID.String()).
					Str("service_name", rpc.ServiceID).
					Str("method_name", rpc.MethodName).
					Msg("rpc_internal_server_error")
				responseHeader.ErrorType = RPCErrInternalServer.Type
				responseHeader.ErrorCode = RPCErrInternalServer.Code
			}

			response = NullMessage
		}

		self.Stream.SendResponse(&responseHeader, response)
	}()
}

func (self *messageProcessor) PostEmitRequest(packet *stream.Packet) {
	requestHeader := &packet.RequestHeader
	value, _ := self.InflightRPCs.Load(requestHeader.SequenceNumber)
	pendingRPC_ := value.(*pendingRPC)

	if packet.Err == nil {
		pendingRPC_.IsEmitted = true
	} else {
		self.InflightRPCs.Delete(requestHeader.SequenceNumber)
		pendingRPC_.Fail(nil, packet.Err)
		packet.Err = stream.ErrPacketDropped
	}
}

func (self *messageProcessor) NewResponse(packet *stream.Packet) {
	responseHeader := &packet.ResponseHeader
	value, ok := self.InflightRPCs.Load(responseHeader.SequenceNumber)

	if !ok {
		packet.Err = stream.ErrPacketDropped
		return
	}

	pendingRPC_ := value.(*pendingRPC)

	if !pendingRPC_.IsEmitted {
		packet.Err = stream.ErrPacketDropped
		return
	}

	self.InflightRPCs.Delete(responseHeader.SequenceNumber)

	if responseHeader.ErrorType == 0 {
		packet.Message = pendingRPC_.NewResponse()
	} else {
		packet.Err = &RPCError{
			Type: responseHeader.ErrorType,
			Code: responseHeader.ErrorCode,
		}
	}

	self.pendingRPCCache = pendingRPC_
}

func (self *messageProcessor) HandleResponse(ctx context.Context, packet *stream.Packet) {
	responseHeader := &packet.ResponseHeader

	if packet.Err == nil {
		self.pendingRPCCache.Succeed(responseHeader.Metadata, packet.Message)
	} else {
		self.pendingRPCCache.Fail(responseHeader.Metadata, packet.Err)
		packet.Err = nil
	}
}

func (self *messageProcessor) PostEmitResponse(packet *stream.Packet) {
}
