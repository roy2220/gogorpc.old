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

type RPCErrorCode = protocol.RPCErrorCode

var (
	ErrRPCBadRequest      = newRPCError(RPCErrorBadRequest, "")
	ErrRPCUnauthorized    = newRPCError(RPCErrorUnauthorized, "")
	ErrRPCForbidden       = newRPCError(RPCErrorForbidden, "")
	ErrRPCNotFound        = newRPCError(RPCErrorNotFound, "")
	ErrRPCTooManyRequests = newRPCError(RPCErrorTooManyRequests, "")

	ErrRPCInternalServer     = newRPCError(RPCErrorInternalServer, "")
	ErrRPCNotImplemented     = newRPCError(RPCErrorNotImplemented, "")
	ErrRPCBadGateway         = newRPCError(RPCErrorBadGateway, "")
	ErrRPCServiceUnavailable = newRPCError(RPCErrorServiceUnavailable, "")
	ErrRPCGatewayTimeout     = newRPCError(RPCErrorGatewayTimeout, "")
)

func NewRPCBadRequestError(reasonCode string) error {
	return newRPCError(RPCErrorBadRequest, reasonCode)
}

func NewRPCForbiddenError(reasonCode string) error {
	return newRPCError(RPCErrorForbidden, reasonCode)
}

func newRPCError(code RPCErrorCode, reasonCode string) error {
	return &RPCError{code, reasonCode}
}
