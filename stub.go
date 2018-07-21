package pbrpc

import (
	"context"
	"reflect"
	"unsafe"

	"github.com/gogo/protobuf/proto"
)

type Channel interface {
	MethodCaller
	Run() error
	Stop()
	UserData() *unsafe.Pointer
}

type MethodCaller interface {
	CallMethod(context.Context, string, string, OutgoingMessage, reflect.Type, bool) (IncomingMessage, error)
	CallMethodWithoutReturn(context.Context, string, string, OutgoingMessage, reflect.Type, bool) error
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
	X_InterceptMethodCall(*MethodRecord, context.Context, Channel, IncomingMessage) (OutgoingMessage, ErrorCode)
}

type MethodTable []MethodRecord

func (self MethodTable) Search(name string) (*MethodRecord, bool) {
	if len(self) >= 1 {
		i := 0
		j := len(self) - 1

		for i < j {
			k := (i + j) / 2

			if (&self[k]).Name < name {
				i = k + 1
			} else {
				j = k
			}
		}

		if methodRecord := &self[i]; methodRecord.Name == name {
			return methodRecord, true
		}
	}

	return nil, false
}

type MethodRecord struct {
	Index        int32
	Name         string
	RequestType  reflect.Type
	ResponseType reflect.Type
	Handler      func(ServiceHandler, context.Context, Channel, IncomingMessage) (OutgoingMessage, error)
}
