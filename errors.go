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
	ErrorChannelBusy    = ErrorCode(protocol.ERROR_CHANNEL_BUSY)
	ErrorNotImplemented = ErrorCode(protocol.ERROR_NOT_IMPLEMENTED)
	ErrorBadRequest     = ErrorCode(protocol.ERROR_BAD_REQUEST)
	ErrorInternalServer = ErrorCode(protocol.ERROR_INTERNAL_SERVER)
	ErrorUserDefined    = ErrorCode(protocol.ERROR_USER_DEFINED)
)

type Error struct {
	isInitiative bool
	code         ErrorCode
	context      string
}

func (self Error) IsInitiative() bool {
	return self.isInitiative
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
	if errorCode <= ErrorUserDefined {
		panic(fmt.Errorf("pbrpc: error reserved: errorCode=%#v", errorCode))
	}

	if _, ok := errorTable[errorCode]; ok {
		panic(fmt.Errorf("pbrpc: error registered: errorCode=%#v", errorCode))
	}

	errorTable[errorCode] = [2]string{errorCodeName, errorDescription}
}

func MakeError(errorCode ErrorCode) error {
	if errorCode <= ErrorUserDefined {
		panic(fmt.Errorf("pbrpc: error reserved: errorCode=%#v", errorCode))
	}

	return Error{true, errorCode, ""}
}

var errorTable = map[ErrorCode][2]string{
	ErrorChannelBroken:         {"ErrorChannelBroken", "channel broken"},
	ErrorChannelTimedOut:       {"ErrorChannelTimedOut", "channel timed out"},
	ErrorPacketPayloadTooLarge: {"ErrorPacketPayloadTooLarge", "packet payload too large"},

	ErrorChannelBusy:    {"ErrorChannelBusy", "channel busy"},
	ErrorNotImplemented: {"ErrorNotImplemented", "not implemented"},
	ErrorBadRequest:     {"ErrorBadRequest", "bad request"},
	ErrorInternalServer: {"ErrorInternalServer", "internal server"},
	ErrorUserDefined:    {"ErrorUserDefined", "user defined"},
}
