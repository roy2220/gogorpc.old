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
	code   ErrorCode
	isMade bool
	desc   string
	data   []byte
}

func (self *Error) GetCode() ErrorCode {
	return self.code
}

func (self *Error) IsMade() bool {
	return self.isMade
}

func (self *Error) GetDesc() string {
	if self.desc != "" {
		return self.desc
	}

	record, ok := errorTable[self.code]
	var desc string

	if ok {
		desc = record[1]
	} else {
		desc = fmt.Sprintf("error %d", self.code)
	}

	return desc
}

func (self *Error) GetData() []byte {
	return self.data
}

func (self *Error) Error() string {
	if self.data == nil {
		return fmt.Sprintf("pbrpc: %s", self.GetDesc())
	} else {
		return fmt.Sprintf("pbrpc: %s: errorData=%q", self.GetDesc(), self.data)
	}
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

func RegisterError(errorCode ErrorCode, errorCodeName string, errorDesc string) {
	if errorCode < ErrorUserDefined {
		panic(fmt.Errorf("pbrpc: error reserved: errorCode=%#v", errorCode))
	}

	if _, ok := errorTable[errorCode]; ok {
		panic(fmt.Errorf("pbrpc: error registered: errorCode=%#v", errorCode))
	}

	if errorCodeName == "" {
		panic(fmt.Errorf("pbrpc: empty error code name: errorCode=%#v", errorCode))
	}

	if errorDesc == "" {
		panic(fmt.Errorf("pbrpc: empty error desc: errorCode=%#v", errorCode))
	}

	errorTable[errorCode] = [2]string{errorCodeName, errorDesc}
}

func MakeError(errorCode ErrorCode, errorData []byte) *Error {
	if errorCode < ErrorUserDefined {
		panic(fmt.Errorf("pbrpc: error reserved: errorCode=%#v", errorCode))
	}

	return X_MakeError(errorCode, "", errorData)
}

func X_MakeError(errorCode ErrorCode, errorDesc string, errorData []byte) *Error {
	return &Error{errorCode, true, errorDesc, errorData}
}

func X_MustGetErrorDesc(errorCode ErrorCode) string {
	return errorTable[errorCode][1]
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
