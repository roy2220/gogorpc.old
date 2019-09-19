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
	Type RPCErrorType
	Code string
}

func (self *RPCError) Error() string {
	message := "pbrpc/channel: rpc: "

	switch self.Type {
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
		message += fmt.Sprintf("error %d", self.Type)
	}

	if self.Code != "" {
		message += " - " + self.Code
	}

	return message
}

func (self *RPCError) Equals(err error) bool {
	if other, ok := err.(*RPCError); ok {
		return *self == *other
	}

	return false
}

type RPCErrorType = protocol.RPCErrorType

var (
	RPCErrBadRequest      = NewRPCError(RPCErrorBadRequest, "")
	RPCErrUnauthorized    = NewRPCError(RPCErrorUnauthorized, "")
	RPCErrForbidden       = NewRPCError(RPCErrorForbidden, "")
	RPCErrNotFound        = NewRPCError(RPCErrorNotFound, "")
	RPCErrTooManyRequests = NewRPCError(RPCErrorTooManyRequests, "")

	RPCErrInternalServer     = NewRPCError(RPCErrorInternalServer, "")
	RPCErrNotImplemented     = NewRPCError(RPCErrorNotImplemented, "")
	RPCErrBadGateway         = NewRPCError(RPCErrorBadGateway, "")
	RPCErrServiceUnavailable = NewRPCError(RPCErrorServiceUnavailable, "")
	RPCErrGatewayTimeout     = NewRPCError(RPCErrorGatewayTimeout, "")
)

func NewRPCError(rpcErrorType RPCErrorType, rpcErrorCode string) *RPCError {
	return &RPCError{rpcErrorType, rpcErrorCode}
}
