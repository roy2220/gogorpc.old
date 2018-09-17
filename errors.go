package pbrpc

import (
	"fmt"

	"github.com/let-z-go/pbrpc/protocol"
)

const (
	ErrorChannelBroken ErrorCode = -(iota + 1)
	ErrorChannelTimedOut
	ErrorPacketPayloadTooLarge
)

const (
	ErrorTooManyRequests = ErrorCode(protocol.ERROR_TOO_MANY_REQUESTS)
	ErrorNotFound        = ErrorCode(protocol.ERROR_NOT_FOUND)
	ErrorBadRequest      = ErrorCode(protocol.ERROR_BAD_REQUEST)
	ErrorNotImplemented  = ErrorCode(protocol.ERROR_NOT_IMPLEMENTED)
	ErrorInternalServer  = ErrorCode(protocol.ERROR_INTERNAL_SERVER)
	ErrorUserDefined     = ErrorCode(protocol.ERROR_USER_DEFINED)
)

type Error struct {
	isPassive bool
	code      ErrorCode
	context   string
}

func (self Error) IsPassive() bool {
	return self.isPassive
}

func (self Error) GetCode() ErrorCode {
	return self.code
}

func (self Error) Error() string {
	errorRecord, ok := errorTable[self.code]
	var result string

	if ok {
		errorDescription := errorRecord[1]
		result = "pbrpc: " + errorDescription
	} else {
		result = fmt.Sprintf("pbrpc: error %d", self.code)
	}

	if self.context != "" {
		result += ": " + self.context
	}

	return result
}

type ErrorCode int32

func (self ErrorCode) GetName() string {
	errorRecord, ok := errorTable[self]

	if ok {
		errorCodeName := errorRecord[0]
		return errorCodeName
	} else {
		return fmt.Sprintf("E%d", self)
	}
}

func (self ErrorCode) GoString() string {
	errorRecord, ok := errorTable[self]

	if ok {
		errorCodeName := errorRecord[0]
		return fmt.Sprintf("<%s>", errorCodeName)
	} else {
		return fmt.Sprintf("<ErrorCode:%d>", self)
	}
}

func RegisterError(errorCode ErrorCode, errorCodeName string, errorDescription string) {
	if errorCode < ErrorUserDefined {
		panic(fmt.Errorf("pbrpc: error reserved: errorCode=%#v", errorCode))
	}

	if _, ok := errorTable[errorCode]; ok {
		panic(fmt.Errorf("pbrpc: error registered: errorCode=%#v", errorCode))
	}

	errorTable[errorCode] = [2]string{errorCodeName, errorDescription}
}

func MakeError(errorCode ErrorCode) error {
	return Error{false, errorCode, ""}
}

var errorTable = map[ErrorCode][2]string{
	ErrorChannelBroken:         {"ErrorChannelBroken", "channel broken"},
	ErrorChannelTimedOut:       {"ErrorChannelTimedOut", "channel timed out"},
	ErrorPacketPayloadTooLarge: {"ErrorPacketPayloadTooLarge", "packet payload too large"},

	ErrorTooManyRequests: {"ErrorTooManyRequests", "too many requests"},
	ErrorNotFound:        {"ErrorNotFound", "not found"},
	ErrorBadRequest:      {"ErrorBadRequest", "bad request"},
	ErrorNotImplemented:  {"ErrorNotImplemented", "not implemented"},
	ErrorInternalServer:  {"ErrorInternalServer", "internal server"},
}
