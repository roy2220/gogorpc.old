package pbrpc

import (
	"context"
	"reflect"
	"unsafe"

	"github.com/gogo/protobuf/proto"
)

type Channel interface {
	CallMethod(context.Context, string, int32, OutgoingMessage, reflect.Type, bool) (IncomingMessage, error)
	CallMethodWithoutReturn(context.Context, string, int32, OutgoingMessage, reflect.Type, bool) error
	Stop()
	UserData() *unsafe.Pointer
}

type IncomingMessage interface {
	proto.Unmarshaler
}

type OutgoingMessage interface {
	proto.Sizer
	MarshalTo([]byte) (int, error)
}

type ServiceHandler interface {
	X_GetName() string
	X_GetMethodTable() MethodTable
	X_InterceptMethodCall(int32, context.Context, Channel, IncomingMessage) (OutgoingMessage, ErrorCode)
}

type MethodTable []struct {
	RequestType  reflect.Type
	ResponseType reflect.Type
	Handler      func(ServiceHandler, context.Context, Channel, IncomingMessage) (OutgoingMessage, ErrorCode)
}
