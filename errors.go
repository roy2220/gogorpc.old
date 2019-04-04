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
	code     ErrorCode
	methodID string
}

func (self Error) GetCode() ErrorCode {
	return self.code
}

func (self Error) IsMade() bool {
	return self.methodID == ""
}

func (self Error) Error() string {
	record, ok := errorTable[self.code]
	var result string

	if ok {
		description := record[1]
		result = "pbrpc: " + description
	} else {
		result = fmt.Sprintf("pbrpc: error %d", self.code)
	}

	if self.methodID != "" {
		result += fmt.Sprintf(" (from %s)", self.methodID)
	}

	return result
}

type ErrorCode int32

func (self ErrorCode) GetName() string {
	record, ok := errorTable[self]

	if ok {
		name := record[0]
		return name
	} else {
		return fmt.Sprintf("E%d", self)
	}
}

func (self ErrorCode) GoString() string {
	record, ok := errorTable[self]

	if ok {
		name := record[0]
		return fmt.Sprintf("<%s>", name)
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

func MakeError(errorCode ErrorCode) Error {
	if errorCode < ErrorUserDefined {
		panic(fmt.Errorf("pbrpc: error reserved: errorCode=%#v", errorCode))
	}

	return X_MakeError(errorCode)
}

func X_MakeError(errorCode ErrorCode) Error {
	return Error{errorCode, ""}
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
