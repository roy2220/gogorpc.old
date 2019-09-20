package channel

import (
	"fmt"

	"github.com/let-z-go/gogorpc/internal/protocol"
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
	return fmt.Sprintf("gogorpc/channel: rpc error: %d - %s", self.Type, self.Code)
}

func (self *RPCError) Equals(err error) bool {
	if other, ok := err.(*RPCError); ok {
		return *self == *other
	}

	return false
}

type RPCErrorType = protocol.RPCErrorType

var (
	RPCErrBadRequest      = NewRPCError(RPCErrorBadRequest, "BadRequest")
	RPCErrUnauthorized    = NewRPCError(RPCErrorUnauthorized, "Unauthorized")
	RPCErrForbidden       = NewRPCError(RPCErrorForbidden, "Forbidden")
	RPCErrNotFound        = NewRPCError(RPCErrorNotFound, "NotFound")
	RPCErrTooManyRequests = NewRPCError(RPCErrorTooManyRequests, "TooManyRequests")

	RPCErrInternalServer     = NewRPCError(RPCErrorInternalServer, "InternalServer")
	RPCErrNotImplemented     = NewRPCError(RPCErrorNotImplemented, "NotImplemented")
	RPCErrBadGateway         = NewRPCError(RPCErrorBadGateway, "BadGateway")
	RPCErrServiceUnavailable = NewRPCError(RPCErrorServiceUnavailable, "ServiceUnavailable")
	RPCErrGatewayTimeout     = NewRPCError(RPCErrorGatewayTimeout, "GatewayTimeout")
)

func NewRPCError(rpcErrorType RPCErrorType, rpcErrorCode string) *RPCError {
	return &RPCError{rpcErrorType, rpcErrorCode}
}
