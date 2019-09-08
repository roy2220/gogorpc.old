package channel

import (
	"fmt"

	"github.com/let-z-go/pbrpc/internal/protocol"
)

const (
	RPCErrorBadRequest      = protocol.RPC_ERROR_BAD_REQUEST
	RPCErrorUnauthorized    = protocol.RPC_ERROR_UNAUTHORIZED
	RPCErrorForbidden       = protocol.RPC_ERROR_FORBIDDEN
	RPCErrorNotFound        = protocol.RPC_ERROR_NOT_FOUND
	RPCErrorTooManyRequests = protocol.RPC_ERROR_TOO_MANY_REQUESTS

	RPCErrorInternalServer     = protocol.RPC_ERROR_INTERNAL_SERVER
	RPCErrorNotImplemented     = protocol.RPC_ERROR_NOT_IMPLEMENTED
	RPCErrorBadGateway         = protocol.RPC_ERROR_BAD_GATEWAY
	RPCErrorServiceUnavailable = protocol.RPC_ERROR_SERVICE_UNAVAILABLE
	RPCErrorGatewayTimeout     = protocol.RPC_ERROR_GATEWAY_TIMEOUT
)

type RPCError struct {
	Code       RPCErrorCode
	ReasonCode string
}

func (self *RPCError) Error() string {
	message := "pbrpc/channel: rpc: "

	switch self.Code {
	case RPCErrorBadRequest:
		message += "bad request"
	case RPCErrorUnauthorized:
		message += "unauthorized"
	case RPCErrorForbidden:
		message += "forbidden"
	case RPCErrorNotFound:
		message += "not found"
	case RPCErrorTooManyRequests:
		message += "too many requests"
	case RPCErrorInternalServer:
		message += "internal server"
	case RPCErrorNotImplemented:
		message += "not implemented"
	case RPCErrorBadGateway:
		message += "bad gateway"
	case RPCErrorServiceUnavailable:
		message += "service unavailable"
	case RPCErrorGatewayTimeout:
		message += "gateway timeout"
	default:
		message += fmt.Sprintf("error %d", self.Code)
	}

	if self.ReasonCode != "" {
		message += " - " + self.ReasonCode
	}

	return message
}

func (self *RPCError) Equals(err error) bool {
	if other, ok := err.(*RPCError); ok {
		return *self == *other
	}

	return false
}

type RPCErrorCode = protocol.RPCErrorCode

var (
	ErrRPCBadRequest      = NewRPCError(RPCErrorBadRequest, "")
	ErrRPCUnauthorized    = NewRPCError(RPCErrorUnauthorized, "")
	ErrRPCForbidden       = NewRPCError(RPCErrorForbidden, "")
	ErrRPCNotFound        = NewRPCError(RPCErrorNotFound, "")
	ErrRPCTooManyRequests = NewRPCError(RPCErrorTooManyRequests, "")

	ErrRPCInternalServer     = NewRPCError(RPCErrorInternalServer, "")
	ErrRPCNotImplemented     = NewRPCError(RPCErrorNotImplemented, "")
	ErrRPCBadGateway         = NewRPCError(RPCErrorBadGateway, "")
	ErrRPCServiceUnavailable = NewRPCError(RPCErrorServiceUnavailable, "")
	ErrRPCGatewayTimeout     = NewRPCError(RPCErrorGatewayTimeout, "")
)

func NewRPCError(rpcErrorCode RPCErrorCode, reasonCode string) *RPCError {
	return &RPCError{rpcErrorCode, reasonCode}
}
