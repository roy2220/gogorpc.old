package pbrpc

import (
	"context"
	"reflect"

	"github.com/gogo/protobuf/proto"
)

type Channel interface {
	Stop()
	CallMethod(context.Context, string, int32, OutgoingMessage, reflect.Type, bool) (IncomingMessage, error)
	CallMethodWithoutReturn(context.Context, string, int32, OutgoingMessage, reflect.Type, bool) error
}

type IncomingMessage interface {
	proto.Unmarshaler
}

type OutgoingMessage interface {
	proto.Sizer
	MarshalTo([]byte) (int, error)
}

type ServiceHandler interface {
	GetName() string
	GetMethodTable() MethodTable
}

type MethodTable []struct {
	RequestType  reflect.Type
	ResponseType reflect.Type
	Handler      func(ServiceHandler, context.Context, Channel, IncomingMessage) (OutgoingMessage, ErrorCode)
}
