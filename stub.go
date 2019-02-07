package pbrpc

import (
	"context"
	"reflect"
)

type ServiceHandler interface {
	X_GetName() string
	X_GetMethodTable() MethodTable
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
	Handler      func(ServiceHandler, context.Context, interface{}) (OutgoingMessage, error)
}
