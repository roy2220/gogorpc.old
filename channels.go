package pbrpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"

	"github.com/let-z-go/toolkit/delay_pool"
	"github.com/let-z-go/toolkit/logger"
	"github.com/let-z-go/toolkit/uuid"
)

type Channel interface {
	MethodCaller
	AddListener(int) (*ChannelListener, error)
	RemoveListener(listener *ChannelListener) error
	Run(context.Context) error
	Stop()
}

type MethodCaller interface {
	CallMethod(context.Context, string, string, int32, []byte, OutgoingMessage, reflect.Type, bool) (interface{}, error)
	CallMethodWithoutReturn(context.Context, string, string, int32, []byte, OutgoingMessage, reflect.Type, bool) error
}

type ClientChannel struct {
	channelBase

	policy          *ClientChannelPolicy
	serverAddresses delay_pool.DelayPool
}

func (self *ClientChannel) Initialize(policy *ClientChannelPolicy, serverAddresses []string) *ClientChannel {
	self.initialize(self, policy.Validate().ChannelPolicy, true)
	self.policy = policy

	if serverAddresses == nil {
		serverAddresses = []string{defaultServerAddress}
	} else {
		if len(serverAddresses) == 0 {
			panic(fmt.Errorf("pbrpc: client channel initialization: serverAddresses=%#v", serverAddresses))
		}
	}

	values := make([]interface{}, len(serverAddresses))

	for i, serverAddress := range serverAddresses {
		values[i] = serverAddress
	}

	self.serverAddresses.Reset(values, 3, self.impl.getTimeout())
	return self
}

func (self *ClientChannel) Run(context_ context.Context) error {
	if self.impl.isClosed() {
		return nil
	}

	context2, cancel2 := context.WithCancel(context_)
	self.stop = cancel2

	var e error

	for {
		var value interface{}
		value, e = self.serverAddresses.GetValue(context2)

		if e != nil {
			break
		}

		context3, cancel3 := context.WithDeadline(context2, self.serverAddresses.WhenNextValueUsable())
		serverAddress := value.(string)
		e = self.impl.connect(self.policy.Connector, context3, serverAddress, self.policy.Handshaker)
		cancel3()

		if e != nil {
			if e != io.EOF {
				if _, ok := e.(*net.OpError); !ok {
					break
				}
			}

			continue
		}

		self.serverAddresses.Reset(nil, 0, self.impl.getTimeout()/3)
		e = self.impl.dispatch(context2, cancel2)

		if e != nil {
			if e != io.EOF {
				if _, ok := e.(*net.OpError); !ok {
					break
				}
			}

			continue
		}
	}

	self.impl.close()
	self.stop = nil
	self.serverAddresses.GC()
	return e
}

type ClientChannelPolicy struct {
	*ChannelPolicy

	Connector  Connector
	Handshaker ClientHandshaker

	validateOnce sync.Once
}

func (self *ClientChannelPolicy) Validate() *ClientChannelPolicy {
	self.validateOnce.Do(func() {
		if self.ChannelPolicy == nil {
			self.ChannelPolicy = &defaultChannelPolicy
		}

		if self.Connector == nil {
			self.Connector = TCPConnector{}
		}

		if self.Handshaker == nil {
			self.Handshaker = shakeHandsWithServer
		}
	})

	return self
}

type ClientHandshaker func(*ClientChannel, context.Context, func(context.Context, *[]byte) error) error

type ServerChannel struct {
	channelBase

	policy     *ServerChannelPolicy
	connection net.Conn
}

func (self *ServerChannel) Initialize(policy *ServerChannelPolicy, connection net.Conn) *ServerChannel {
	self.initialize(self, policy.Validate().ChannelPolicy, false)
	self.policy = policy
	self.connection = connection
	return self
}

func (self *ServerChannel) Run(context_ context.Context) error {
	if self.impl.isClosed() {
		return nil
	}

	context2, cancel2 := context.WithCancel(context_)
	self.stop = cancel2

	cleanup := func() {
		self.impl.close()
		self.stop = nil
		self.connection = nil
	}

	if e := self.impl.accept(context2, self.connection, self.policy.Handshaker); e != nil {
		cleanup()
		return e
	}

	e := self.impl.dispatch(context2, cancel2)
	cleanup()
	return e
}

type ServerChannelPolicy struct {
	*ChannelPolicy

	Handshaker ServerHandshaker

	validateOnce sync.Once
}

func (self *ServerChannelPolicy) Validate() *ServerChannelPolicy {
	self.validateOnce.Do(func() {
		if self.ChannelPolicy == nil {
			self.ChannelPolicy = &defaultChannelPolicy
		}

		if self.Handshaker == nil {
			self.Handshaker = shakeHandsWithClient
		}
	})

	return self
}

type ServerHandshaker func(*ServerChannel, context.Context, *[]byte) (bool, error)

type ContextVars struct {
	Channel      Channel
	ServiceName  string
	MethodName   string
	MethodIndex  int32
	ExtraData    []byte
	TraceID      uuid.UUID
	SpanParentID int32
	SpanID       int32

	logger             *logger.Logger
	sequenceNumber     int32
	bufferOfNextSpanID int32
	nextSpanID         *int32
}

type RawMessage []byte

func (self *RawMessage) Unmarshal(data []byte) error {
	*self = make([]byte, len(data))
	copy(*self, data)
	return nil
}

func (self RawMessage) Size() int {
	return len(self)
}

func (self RawMessage) MarshalTo(buffer []byte) (int, error) {
	return copy(buffer, self), nil
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

type channelBase struct {
	impl channelImpl
	stop context.CancelFunc
}

func (self *channelBase) AddListener(maxNumberOfStateChanges int) (*ChannelListener, error) {
	return self.impl.addListener(maxNumberOfStateChanges)
}

func (self *channelBase) RemoveListener(listener *ChannelListener) error {
	return self.impl.removeListener(listener)
}

func (self *channelBase) Stop() {
	self.stop()
}

func (self *channelBase) CallMethod(
	context_ context.Context,
	serviceName string,
	methodName string,
	methodIndex int32,
	extraData []byte,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) (interface{}, error) {
	contextVars_ := ContextVars{
		Channel:     self.impl.holder,
		ServiceName: serviceName,
		MethodName:  methodName,
		MethodIndex: methodIndex,
		ExtraData:   extraData,
	}

	if parentContextVars, ok := GetContextVars(context_); ok {
		contextVars_.TraceID = parentContextVars.TraceID
		contextVars_.SpanParentID = parentContextVars.SpanID
		contextVars_.SpanID = *parentContextVars.nextSpanID
		*parentContextVars.nextSpanID++
		contextVars_.nextSpanID = parentContextVars.nextSpanID
	} else {
		traceID, e := uuid.GenerateUUID4()

		if e != nil {
			return nil, e
		}

		contextVars_.TraceID = traceID
		contextVars_.SpanParentID = 0
		contextVars_.SpanID = 1
		contextVars_.bufferOfNextSpanID = 2
		contextVars_.nextSpanID = &contextVars_.bufferOfNextSpanID
	}

	outgoingMethodInterceptors := make([]OutgoingMethodInterceptor, 0, normalNumberOfMethodInterceptors)
	outgoingMethodInterceptors = append(outgoingMethodInterceptors, self.impl.policy.outgoingMethodInterceptors[""]...)
	outgoingMethodInterceptors = append(outgoingMethodInterceptors, self.impl.policy.outgoingMethodInterceptors[contextVars_.ServiceName]...)

	if contextVars_.MethodIndex >= 0 {
		methodInterceptorLocator := makeMethodInterceptorLocator(contextVars_.ServiceName, contextVars_.MethodIndex)
		outgoingMethodInterceptors = append(outgoingMethodInterceptors, self.impl.policy.outgoingMethodInterceptors[methodInterceptorLocator]...)
	}

	n := len(outgoingMethodInterceptors)

	if n == 0 {
		return self.callMethod(bindContextVars(context_, &contextVars_), &contextVars_, request, responseType, autoRetryMethodCall)
	}

	i := 0
	var outgoingMethodDispatcher OutgoingMethodDispatcher

	outgoingMethodDispatcher = func(context_ context.Context, request OutgoingMessage) (interface{}, error) {
		if i == n {
			return self.callMethod(context_, &contextVars_, request, responseType, autoRetryMethodCall)
		} else {
			outgoingMethodInterceptor := outgoingMethodInterceptors[i]
			i++
			return outgoingMethodInterceptor(context_, request, outgoingMethodDispatcher)
		}
	}

	return outgoingMethodDispatcher(bindContextVars(context_, &contextVars_), request)
}

func (self *channelBase) CallMethodWithoutReturn(
	context_ context.Context,
	serviceName string,
	methodName string,
	methodIndex int32,
	extraData []byte,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) error {
	traceID, e := uuid.GenerateUUID4()

	if e != nil {
		return e
	}

	contextVars_ := ContextVars{
		Channel:      self.impl.holder,
		ServiceName:  serviceName,
		MethodName:   methodName,
		MethodIndex:  methodIndex,
		ExtraData:    extraData,
		TraceID:      traceID,
		SpanParentID: 0,
		SpanID:       1,

		bufferOfNextSpanID: 2,
	}

	contextVars_.nextSpanID = &contextVars_.bufferOfNextSpanID
	outgoingMethodInterceptors := make([]OutgoingMethodInterceptor, 0, normalNumberOfMethodInterceptors)
	outgoingMethodInterceptors = append(outgoingMethodInterceptors, self.impl.policy.outgoingMethodInterceptors[""]...)
	outgoingMethodInterceptors = append(outgoingMethodInterceptors, self.impl.policy.outgoingMethodInterceptors[contextVars_.ServiceName]...)

	if contextVars_.MethodIndex >= 0 {
		methodInterceptorLocator := makeMethodInterceptorLocator(contextVars_.ServiceName, contextVars_.MethodIndex)
		outgoingMethodInterceptors = append(outgoingMethodInterceptors, self.impl.policy.outgoingMethodInterceptors[methodInterceptorLocator]...)
	}

	n := len(outgoingMethodInterceptors)

	if n == 0 {
		return self.callMethodWithoutReturn(bindContextVars(context_, &contextVars_), &contextVars_, request, responseType, autoRetryMethodCall)
	}

	i := 0
	var outgoingMethodDispatcher OutgoingMethodDispatcher

	outgoingMethodDispatcher = func(context_ context.Context, request OutgoingMessage) (interface{}, error) {
		if i == n {
			return nil, self.callMethodWithoutReturn(context_, &contextVars_, request, responseType, autoRetryMethodCall)
		} else {
			outgoingMethodInterceptor := outgoingMethodInterceptors[i]
			i++
			return outgoingMethodInterceptor(context_, request, outgoingMethodDispatcher)
		}
	}

	_, e = outgoingMethodDispatcher(bindContextVars(context_, &contextVars_), request)
	return e
}

func (self *channelBase) initialize(sub Channel, policy *ChannelPolicy, isClientSide bool) *channelBase {
	self.impl.initialize(sub, policy, isClientSide)
	return self
}

func (self *channelBase) callMethod(
	context_ context.Context,
	contextVars_ *ContextVars,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) (interface{}, error) {
	var response interface{}
	error_ := make(chan error, 1)

	callback := func(response2 interface{}, errorCode ErrorCode) {
		if errorCode != 0 {
			error_ <- Error{errorCode, makeMethodID(contextVars_.ServiceName, contextVars_.MethodName)}
			return
		}

		response = response2
		error_ <- nil
	}

	if e := self.impl.callMethod(
		context_,
		contextVars_,
		request,
		responseType,
		autoRetryMethodCall,
		callback,
	); e != nil {
		return nil, e
	}

	select {
	case e := <-error_:
		if e != nil {
			return nil, e
		}
	case <-context_.Done():
		return nil, context_.Err()
	}

	return response, nil
}

func (self *channelBase) callMethodWithoutReturn(
	context_ context.Context,
	contextVars_ *ContextVars,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) error {
	return self.impl.callMethod(
		context_,
		contextVars_,
		request,
		responseType,
		autoRetryMethodCall,
		func(interface{}, ErrorCode) {},
	)
}

var defaultChannelPolicy ChannelPolicy

func shakeHandsWithServer(_ *ClientChannel, context_ context.Context, greeter func(context.Context, *[]byte) error) error {
	handshake := []byte(nil)
	return greeter(context_, &handshake)
}

func shakeHandsWithClient(*ServerChannel, context.Context, *[]byte) (bool, error) {
	return true, nil
}

func bindContextVars(context_ context.Context, contextVars_ *ContextVars) context.Context {
	return context.WithValue(context_, contextVars{}, contextVars_)
}
