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
	AddListener(maxNumberOfStateChanges int) (listener *ChannelListener, e error)
	RemoveListener(listener *ChannelListener) (e error)
	Run(context_ context.Context) (e error)
	Stop()
}

type MethodCaller interface {
	CallMethod(context_ context.Context,
		serviceName string,
		methodName string,
		methodIndex int32,
		resourceID string,
		extraData map[string][]byte,
		request OutgoingMessage,
		responseType reflect.Type,
		autoRetryMethodCall bool) (response interface{}, e error)
	CallMethodWithoutReturn(context_ context.Context,
		serviceName string,
		methodName string,
		methodIndex int32,
		resourceID string,
		extraData map[string][]byte,
		request OutgoingMessage,
		responseType reflect.Type,
		autoRetryMethodCall bool) (e error)
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

	self.serverAddresses.Reset(values, 3, self.impl.GetTimeout())
	return self
}

func (self *ClientChannel) CallMethod(
	context_ context.Context,
	serviceName string,
	methodName string,
	methodIndex int32,
	resourceID string,
	extraData map[string][]byte,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) (interface{}, error) {
	contextVars_ := makeContextVars(self, serviceName, methodName, methodIndex, resourceID, extraData, context_)
	outgoingMethodInterceptors := makeOutgoingMethodInterceptors(self.policy.ChannelPolicy, contextVars_)
	return self.callMethod(outgoingMethodInterceptors, context_, contextVars_, request, responseType, autoRetryMethodCall)
}

func (self *ClientChannel) CallMethodWithoutReturn(
	context_ context.Context,
	serviceName string,
	methodName string,
	methodIndex int32,
	resourceID string,
	extraData map[string][]byte,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) error {
	contextVars_ := makeContextVars(self, serviceName, methodName, methodIndex, resourceID, extraData, context_)
	outgoingMethodInterceptors := makeOutgoingMethodInterceptors(self.policy.ChannelPolicy, contextVars_)
	return self.callMethodWithoutReturn(outgoingMethodInterceptors, context_, contextVars_, request, responseType, autoRetryMethodCall)
}

func (self *ClientChannel) Run(context_ context.Context) error {
	if self.impl.IsClosed() {
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
		e = self.impl.Connect(self.policy.Connector, context3, serverAddress, self.policy.Handshaker)
		cancel3()

		if e != nil {
			if e != io.EOF {
				if _, ok := e.(*net.OpError); !ok {
					break
				}
			}

			continue
		}

		self.serverAddresses.Reset(nil, 0, self.impl.GetTimeout()/3)
		context3, cancel3 = context.WithCancel(context2)
		e = self.impl.Dispatch(context3, cancel3, -1)

		if e != nil {
			if e != io.EOF {
				if _, ok := e.(*net.OpError); !ok {
					break
				}
			}

			continue
		}
	}

	self.impl.Close()
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
	creatorID  int32
	connection net.Conn
}

func (self *ServerChannel) Initialize(policy *ServerChannelPolicy, creatorID int32, connection net.Conn) *ServerChannel {
	self.initialize(self, policy.Validate().ChannelPolicy, false)
	self.policy = policy
	self.creatorID = creatorID
	self.connection = connection
	return self
}

func (self *ServerChannel) CallMethod(
	context_ context.Context,
	serviceName string,
	methodName string,
	methodIndex int32,
	resourceID string,
	extraData map[string][]byte,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) (interface{}, error) {
	contextVars_ := makeContextVars(self, serviceName, methodName, methodIndex, resourceID, extraData, context_)
	outgoingMethodInterceptors := makeOutgoingMethodInterceptors(self.policy.ChannelPolicy, contextVars_)
	return self.callMethod(outgoingMethodInterceptors, context_, contextVars_, request, responseType, autoRetryMethodCall)
}

func (self *ServerChannel) CallMethodWithoutReturn(
	context_ context.Context,
	serviceName string,
	methodName string,
	methodIndex int32,
	resourceID string,
	extraData map[string][]byte,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) error {
	contextVars_ := makeContextVars(self, serviceName, methodName, methodIndex, resourceID, extraData, context_)
	outgoingMethodInterceptors := makeOutgoingMethodInterceptors(self.policy.ChannelPolicy, contextVars_)
	return self.callMethodWithoutReturn(outgoingMethodInterceptors, context_, contextVars_, request, responseType, autoRetryMethodCall)
}

func (self *ServerChannel) Run(context_ context.Context) error {
	if self.impl.IsClosed() {
		return nil
	}

	context2, cancel2 := context.WithCancel(context_)
	self.stop = cancel2

	cleanup := func() {
		self.impl.Close()
		self.stop = nil
		self.connection = nil
	}

	if e := self.impl.Accept(context2, self.creatorID, self.connection, self.policy.Handshaker); e != nil {
		cleanup()
		return e
	}

	e := self.impl.Dispatch(context2, cancel2, self.creatorID)
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
	ServerID    int32
	Channel     Channel
	RequestID   uuid.UUID
	ServiceName string
	MethodName  string
	MethodIndex int32
	ResourceID  string
	ExtraData   map[string][]byte

	logger *logger.Logger
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
	contextVars_, ok := context_.Value(contextVars{}).(*ContextVars)
	return contextVars_, ok
}

func MustGetContextVars(context_ context.Context) *ContextVars {
	return context_.Value(contextVars{}).(*ContextVars)
}

type channelBase struct {
	impl channelImpl
	stop context.CancelFunc
}

func (self *channelBase) AddListener(maxNumberOfStateChanges int) (*ChannelListener, error) {
	return self.impl.AddListener(maxNumberOfStateChanges)
}

func (self *channelBase) RemoveListener(listener *ChannelListener) error {
	return self.impl.RemoveListener(listener)
}

func (self *channelBase) Stop() {
	self.stop()
}

func (self *channelBase) initialize(sub Channel, policy *ChannelPolicy, isClientSide bool) *channelBase {
	self.impl.Initialize(sub, policy, isClientSide)
	return self
}

func (self *channelBase) callMethod(
	outgoingMethodInterceptors []OutgoingMethodInterceptor,
	context_ context.Context,
	contextVars_ *ContextVars,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) (interface{}, error) {
	n := len(outgoingMethodInterceptors)

	if n == 0 {
		poolOfOutgoingMethodInterceptors.Put(outgoingMethodInterceptors)
		return self.doCallMethod(bindContextVars(context_, contextVars_), contextVars_, request, responseType, autoRetryMethodCall)
	}

	i := 0
	var outgoingMethodHandler OutgoingMethodHandler

	outgoingMethodHandler = func(context_ context.Context, request OutgoingMessage) (interface{}, error) {
		if i == n {
			poolOfOutgoingMethodInterceptors.Put(outgoingMethodInterceptors)
			return self.doCallMethod(context_, contextVars_, request, responseType, autoRetryMethodCall)
		} else {
			outgoingMethodInterceptor := outgoingMethodInterceptors[i]
			i++
			return outgoingMethodInterceptor(context_, request, outgoingMethodHandler)
		}
	}

	return outgoingMethodHandler(bindContextVars(context_, contextVars_), request)
}

func (self *channelBase) doCallMethod(
	context_ context.Context,
	contextVars_ *ContextVars,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) (interface{}, error) {
	var response interface{}
	error_ := make(chan error, 1)

	callback := func(response2 interface{}, errorCode ErrorCode, errorDesc string, errorData []byte) {
		if errorCode != 0 {
			error_ <- &Error{code: errorCode, desc: errorDesc, data: errorData}
			return
		}

		response = response2
		error_ <- nil
	}

	if e := self.impl.CallMethod(
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
	outgoingMethodInterceptors []OutgoingMethodInterceptor,
	context_ context.Context,
	contextVars_ *ContextVars,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) error {
	n := len(outgoingMethodInterceptors)

	if n == 0 {
		poolOfOutgoingMethodInterceptors.Put(outgoingMethodInterceptors)
		return self.doCallMethodWithoutReturn(bindContextVars(context_, contextVars_), contextVars_, request, responseType, autoRetryMethodCall)
	}

	i := 0
	var outgoingMethodHandler OutgoingMethodHandler

	outgoingMethodHandler = func(context_ context.Context, request OutgoingMessage) (interface{}, error) {
		if i == n {
			poolOfOutgoingMethodInterceptors.Put(outgoingMethodInterceptors)
			return nil, self.doCallMethodWithoutReturn(context_, contextVars_, request, responseType, autoRetryMethodCall)
		} else {
			outgoingMethodInterceptor := outgoingMethodInterceptors[i]
			i++
			return outgoingMethodInterceptor(context_, request, outgoingMethodHandler)
		}
	}

	_, e := outgoingMethodHandler(bindContextVars(context_, contextVars_), request)
	return e
}

func (self *channelBase) doCallMethodWithoutReturn(
	context_ context.Context,
	contextVars_ *ContextVars,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) error {
	return self.impl.CallMethod(
		context_,
		contextVars_,
		request,
		responseType,
		autoRetryMethodCall,
		func(interface{}, ErrorCode, string, []byte) {},
	)
}

var defaultChannelPolicy ChannelPolicy
var poolOfOutgoingMethodInterceptors = sync.Pool{New: func() interface{} { return make([]OutgoingMethodInterceptor, normalNumberOfMethodInterceptors) }}

func makeContextVars(
	sub Channel,
	serviceName string,
	methodName string,
	methodIndex int32,
	resourceID string,
	extraData map[string][]byte,
	context_ context.Context,
) *ContextVars {
	contextVars_ := ContextVars{
		Channel:     sub,
		ServiceName: serviceName,
		MethodName:  methodName,
		MethodIndex: methodIndex,
		ResourceID:  resourceID,
		ExtraData:   extraData,
	}

	parentContextVars, ok := GetContextVars(context_)

	if ok {
		contextVars_.ServerID = parentContextVars.ServerID
		contextVars_.RequestID = parentContextVars.RequestID
	} else {
		contextVars_.ServerID = -1
		contextVars_.RequestID = uuid.GenerateUUID4Fast()
	}

	return &contextVars_
}

func makeOutgoingMethodInterceptors(policy *ChannelPolicy, contextVars_ *ContextVars) []OutgoingMethodInterceptor {
	outgoingMethodInterceptors := poolOfOutgoingMethodInterceptors.Get().([]OutgoingMethodInterceptor)[:0]
	outgoingMethodInterceptors = append(outgoingMethodInterceptors, policy.outgoingMethodInterceptors[""]...)
	outgoingMethodInterceptors = append(outgoingMethodInterceptors, policy.outgoingMethodInterceptors[contextVars_.ServiceName]...)

	if contextVars_.MethodIndex >= 0 {
		methodInterceptorLocator := makeMethodInterceptorLocator(contextVars_.ServiceName, contextVars_.MethodIndex)
		outgoingMethodInterceptors = append(outgoingMethodInterceptors, policy.outgoingMethodInterceptors[methodInterceptorLocator]...)
	}

	return outgoingMethodInterceptors
}

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
