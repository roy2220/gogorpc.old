package channel

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/let-z-go/gogorpc/internal/protocol"
	"github.com/let-z-go/gogorpc/internal/stream"
	"github.com/let-z-go/toolkit/uuid"
)

var ErrFastResponse = errors.New("gogorpc/channel: fast response")

type messageProcessor struct {
	Keepaliver   Keepaliver
	Options      *Options
	Stream       *stream.Stream
	InflightRPCs *sync.Map

	methodOptionsCache *MethodOptions
	pendingRPCCache    *pendingRPC
}

var _ = stream.MessageProcessor((*messageProcessor)(nil))

func (self *messageProcessor) NewKeepalive(packet *Packet) {
	packet.Message = self.Keepaliver.NewKeepalive()
}

func (self *messageProcessor) HandleKeepalive(ctx context.Context, packet *Packet) {
	packet.Err = self.Keepaliver.HandleKeepalive(ctx, packet.Message)
}

func (self *messageProcessor) EmitKeepalive(packet *Packet) {
	packet.Message, packet.Err = self.Keepaliver.EmitKeepalive()
}

func (self *messageProcessor) NewRequest(packet *Packet) {
	methodOptions := self.Options.GetMethod(packet.RequestHeader.ServiceId, packet.RequestHeader.MethodName)
	packet.Message = methodOptions.RequestFactory()
	self.methodOptionsCache = methodOptions
}

func (self *messageProcessor) HandleRequest(ctx context.Context, packet *Packet) {
	requestHeader := &packet.RequestHeader
	traceID := uuid.UUID{requestHeader.TraceId.Low, requestHeader.TraceId.High}

	if packet.Err != nil {
		if packet.Err == ErrFastResponse {
			// fast response by MessageFilter.FilterIncomingRequest
			responseHeader := &packet.ResponseHeader
			responseHeader.SequenceNumber = requestHeader.SequenceNumber

			if packet.ResponseHeader.ErrorType == 0 {
				self.Stream.SendResponse(responseHeader, packet.Message)
			} else {
				self.Stream.SendResponse(responseHeader, NullMessage)
			}
		} else {
			self.Options.Logger.Info().Err(packet.Err).
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
		self.Options.Logger.Info().
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
		rpc.Ctx = BindRPC(rpc.Ctx, rpc)
		rpc.Handle()

		responseHeader := protocol.ResponseHeader{
			SequenceNumber: rpc.internals.SequenceNumber,
			Metadata:       rpc.ResponseMetadata,
		}

		var response Message

		if rpc.Err == nil {
			response = rpc.Response
		} else {
			var rpcErr *RPCError

			if rpcErr2, ok := rpc.Err.(*RPCError); ok {
				rpcErr = rpcErr2
			} else {
				self.Options.Logger.Error().Err(rpc.Err).
					Str("transport_id", self.Stream.TransportID().String()).
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
		self.Stream.SendResponse(&responseHeader, response)
	}()
}

func (self *messageProcessor) PostEmitRequest(packet *Packet) {
	requestHeader := &packet.RequestHeader
	value, _ := self.InflightRPCs.Load(requestHeader.SequenceNumber)
	pendingRPC_ := value.(*pendingRPC)

	if packet.Err == nil {
		pendingRPC_.IsEmitted = true
	} else {
		self.InflightRPCs.Delete(requestHeader.SequenceNumber)

		if packet.Err == ErrFastResponse {
			// fast response by MessageFilter.FilterOutgoingRequest
			responseHeader := &packet.ResponseHeader

			if responseHeader.ErrorType == 0 {
				self.pendingRPCCache.Succeed(responseHeader.Metadata, packet.Message)
			} else {
				pendingRPC_.Fail(responseHeader.Metadata, &RPCError{
					Type: responseHeader.ErrorType,
					Code: responseHeader.ErrorCode,
				})
			}
		} else {
			pendingRPC_.Fail(nil, packet.Err)
		}

		packet.Err = ErrPacketDropped
	}
}

func (self *messageProcessor) NewResponse(packet *Packet) {
	responseHeader := &packet.ResponseHeader
	value, ok := self.InflightRPCs.Load(responseHeader.SequenceNumber)

	if !ok {
		packet.Err = ErrPacketDropped
		return
	}

	pendingRPC_ := value.(*pendingRPC)

	if !pendingRPC_.IsEmitted {
		packet.Err = ErrPacketDropped
		return
	}

	self.InflightRPCs.Delete(responseHeader.SequenceNumber)

	if responseHeader.ErrorType == 0 {
		packet.Message = pendingRPC_.NewResponse()
	} else {
		packet.Message = NullMessage
	}

	self.pendingRPCCache = pendingRPC_
}

func (self *messageProcessor) HandleResponse(ctx context.Context, packet *Packet) {
	responseHeader := &packet.ResponseHeader

	if packet.Err == nil {
		if responseHeader.ErrorType == 0 {
			self.pendingRPCCache.Succeed(responseHeader.Metadata, packet.Message)
		} else {
			self.pendingRPCCache.Fail(responseHeader.Metadata, &RPCError{
				Type: responseHeader.ErrorType,
				Code: responseHeader.ErrorCode,
			})
		}
	} else {
		self.pendingRPCCache.Fail(responseHeader.Metadata, packet.Err)
		packet.Err = nil
	}
}

func (self *messageProcessor) PostEmitResponse(packet *Packet) {
}
