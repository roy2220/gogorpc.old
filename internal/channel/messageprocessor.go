package channel

import (
	"context"
	"sync"
	"time"

	"github.com/let-z-go/pbrpc/internal/protocol"
	"github.com/let-z-go/pbrpc/internal/stream"
	"github.com/let-z-go/toolkit/uuid"
)

type messageProcessor struct {
	Options     *Options
	Stream      *stream.Stream
	PendingRPCs *sync.Map

	methodOptionsCache *MethodOptions
	pendingRPCCache    *pendingRPC
}

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
	methodOptions := self.Options.GetMethod(packet.RequestHeader.ServiceName, packet.RequestHeader.MethodName)
	packet.Message = methodOptions.RequestFactory()
	self.methodOptionsCache = methodOptions
}

func (self *messageProcessor) HandleRequest(ctx context.Context, packet *stream.Packet) {
	traceID := uuid.UUID{packet.RequestHeader.TraceId.Low, packet.RequestHeader.TraceId.High}

	if packet.Err != nil {
		self.Options.Logger().Info().Err(packet.Err).
			Str("transport_id", self.Stream.GetTransportID().String()).
			Str("trace_id", traceID.String()).
			Str("service_name", packet.RequestHeader.ServiceName).
			Str("method_name", packet.RequestHeader.MethodName).
			Msg("rpc_bad_request")
		packet.Err = nil

		self.Stream.SendResponse(&protocol.ResponseHeader{
			SequenceNumber: packet.RequestHeader.SequenceNumber,
			ErrorCode:      RPCErrorBadRequest,
		}, stream.NullMessage)

		return
	}

	if self.methodOptionsCache.IncomingRPCHandler == nil {
		self.Options.Logger().Info().
			Str("transport_id", self.Stream.GetTransportID().String()).
			Str("trace_id", traceID.String()).
			Str("service_name", packet.RequestHeader.ServiceName).
			Str("method_name", packet.RequestHeader.MethodName).
			Msg("rpc_not_found")

		self.Stream.SendResponse(&protocol.ResponseHeader{
			SequenceNumber: packet.RequestHeader.SequenceNumber,
			ErrorCode:      RPCErrorNotFound,
		}, stream.NullMessage)

		return
	}

	rpc := RPC{
		Ctx:            ctx,
		TraceID:        traceID,
		ServiceName:    packet.RequestHeader.ServiceName,
		MethodName:     packet.RequestHeader.MethodName,
		InputExtraData: packet.RequestHeader.ExtraData,
		Request:        packet.Message,

		internals: *(&RPCInternals{
			SequenceNumber: packet.RequestHeader.SequenceNumber,
			Deadline:       packet.RequestHeader.Deadline,
		}).Init(self.methodOptionsCache.IncomingRPCHandler, self.methodOptionsCache.IncomingRPCInterceptors),
	}

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
			ExtraData:      rpc.OutputExtraData,
		}

		var response stream.Message

		if rpc.Err == nil {
			response = rpc.Response
		} else {
			if error_, ok := rpc.Err.(*RPCError); ok {
				responseHeader.ErrorCode = error_.Code
				responseHeader.ReasonCode = error_.ReasonCode
			} else {
				self.Options.Logger().Error().Err(rpc.Err).
					Str("transport_id", self.Stream.GetTransportID().String()).
					Str("trace_id", rpc.TraceID.String()).
					Str("service_name", rpc.ServiceName).
					Str("method_name", rpc.MethodName).
					Msg("rpc_internal_server_error")
				responseHeader.ErrorCode = RPCErrorInternalServer
			}

			response = stream.NullMessage
		}

		self.Stream.SendResponse(&responseHeader, response)
	}()
}

func (self *messageProcessor) PostEmitRequest(packet *stream.Packet) {
	value, _ := self.PendingRPCs.Load(packet.RequestHeader.SequenceNumber)
	pendingRPC_ := value.(*pendingRPC)

	if packet.Err == nil {
		pendingRPC_.IsEmitted = true
	} else {
		self.PendingRPCs.Delete(packet.RequestHeader.SequenceNumber)
		pendingRPC_.Fail(nil, packet.Err)
		packet.Err = stream.ErrPacketDropped
	}
}

func (self *messageProcessor) NewResponse(packet *stream.Packet) {
	value, ok := self.PendingRPCs.Load(packet.ResponseHeader.SequenceNumber)

	if !ok {
		packet.Err = stream.ErrPacketDropped
		return
	}

	pendingRPC_ := value.(*pendingRPC)

	if !pendingRPC_.IsEmitted {
		packet.Err = stream.ErrPacketDropped
		return
	}

	self.PendingRPCs.Delete(packet.RequestHeader.SequenceNumber)
	packet.Message = pendingRPC_.NewResponse()
	self.pendingRPCCache = pendingRPC_
}

func (self *messageProcessor) HandleResponse(ctx context.Context, packet *stream.Packet) {
	if packet.Err == nil {
		if packet.ResponseHeader.ErrorCode == 0 {
			self.pendingRPCCache.Fail(packet.ResponseHeader.ExtraData, &RPCError{
				Code:       packet.ResponseHeader.ErrorCode,
				ReasonCode: packet.ResponseHeader.ReasonCode,
			})
		} else {
			self.pendingRPCCache.Succeed(packet.ResponseHeader.ExtraData, packet.Message)
		}
	} else {
		self.pendingRPCCache.Fail(packet.ResponseHeader.ExtraData, packet.Err)
		packet.Err = nil
	}
}

func (self *messageProcessor) PostEmitResponse(packet *stream.Packet) {
}
