package pbrpc

import (
	"context"
	"reflect"
	"unsafe"

	"github.com/let-z-go/toolkit/logger"

	"github.com/gogo/protobuf/proto"
)

type ServiceHandler interface {
	X_RegisterMethodInterceptor(MethodInterceptor)
	X_HandleMethod(*MethodHandlingInfo) (OutgoingMessage, ErrorCode)
	X_GetName() string
	X_GetMethodTable() MethodTable
}

type MethodInterceptor func(*MethodHandlingInfo, MethodHandler) (OutgoingMessage, ErrorCode)
type MethodHandler func(*MethodHandlingInfo) (OutgoingMessage, ErrorCode)

type MethodHandlingInfo struct {
	ServiceHandler ServiceHandler
	MethodRecord   *MethodRecord
	Context        context.Context
	ContextVars    ContextVars
	Request        interface{}

	logger *logger.Logger
}

type ContextVars struct {
	Channel Channel
}

type Channel interface {
	MethodCaller
	AddListener(int) (*ChannelListener, error)
	RemoveListener(listener *ChannelListener) error
	Run() error
	Stop()
	GetIDString() string
}

type MethodCaller interface {
	CallMethod(context.Context, string, string, OutgoingMessage, reflect.Type, bool) (interface{}, error)
	CallMethodWithoutReturn(context.Context, string, string, OutgoingMessage, reflect.Type, bool) error
}

type IncomingMessage interface {
	proto.Unmarshaler
}

type OutgoingMessage interface {
	proto.Sizer
	MarshalTo([]byte) (int, error)
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

type ServiceHandlerBase struct {
	methodInterceptors []MethodInterceptor
}

func (self *ServiceHandlerBase) X_RegisterMethodInterceptor(methodInterceptor MethodInterceptor) {
	self.methodInterceptors = append(self.methodInterceptors, methodInterceptor)
}

func (self *ServiceHandlerBase) X_HandleMethod(methodHandlingInfo *MethodHandlingInfo) (OutgoingMessage, ErrorCode) {
	if self.methodInterceptors == nil {
		return handleMethod(methodHandlingInfo)
	}

	methodInterceptor := self.methodInterceptors[0]
	nextMethodInterceptorIndex := 1
	var methodHandler MethodHandler

	methodHandler = func(methodHandlingInfo *MethodHandlingInfo) (OutgoingMessage, ErrorCode) {
		if nextMethodInterceptorIndex == len(self.methodInterceptors) {
			return handleMethod(methodHandlingInfo)
		}

		nextMethodInterceptor := self.methodInterceptors[nextMethodInterceptorIndex]
		nextMethodInterceptorIndex++
		return nextMethodInterceptor(methodHandlingInfo, methodHandler)
	}

	return methodInterceptor(methodHandlingInfo, methodHandler)
}

func RegisterMethodInterceptors(serviceHandler ServiceHandler, methodInterceptors ...MethodInterceptor) ServiceHandler {
	for _, methodInterceptor := range methodInterceptors {
		serviceHandler.X_RegisterMethodInterceptor(methodInterceptor)
	}

	return serviceHandler
}

func GetContextVars(context_ context.Context) (*ContextVars, bool) {
	value := context_.Value(contextVars{})

	if value == nil {
		return nil, false
	}

	return value.(*ContextVars), true
}

func MustGetContextVars(context_ context.Context) *ContextVars {
	return context_.Value(contextVars{}).(*ContextVars)
}

type contextVars struct{}

func bindContextVars(context_ context.Context, contextVars_ *ContextVars) context.Context {
	return context.WithValue(context_, contextVars{}, contextVars_)
}

func handleMethod(methodHandlingInfo *MethodHandlingInfo) (OutgoingMessage, ErrorCode) {
	response, e := methodHandlingInfo.MethodRecord.Handler(methodHandlingInfo.ServiceHandler, methodHandlingInfo.Context, methodHandlingInfo.Request)
	var errorCode ErrorCode

	if e == nil {
		errorCode = 0
	} else {
		if e2, ok := e.(Error); ok && !e2.isPassive {
			errorCode = e2.code
		} else {
			channel := methodHandlingInfo.ContextVars.Channel
			serviceName := methodHandlingInfo.ServiceHandler.X_GetName()
			methodName := methodHandlingInfo.MethodRecord.Name
			methodHandlingInfo.logger.Errorf("internal server error: channelID=%#v, methodID=%v, request=%#v, e=%#v", channel.GetIDString(), representMethodID(serviceName, methodName), methodHandlingInfo.Request, e.Error())
			errorCode = ErrorInternalServer
		}
	}

	return response, errorCode
}
