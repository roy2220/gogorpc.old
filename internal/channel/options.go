package channel

import (
	"context"
	"sync"

	"github.com/let-z-go/pbrpc/internal/stream"
	"github.com/let-z-go/toolkit/utils"
	"github.com/rs/zerolog"
)

type Options struct {
	Handshaker stream.Handshaker
	Keepaliver Keepaliver
	Stream     *stream.Options

	serviceOptionsManager

	normalizeOnce sync.Once
	logger        *zerolog.Logger
}

func (self *Options) Normalize() *Options {
	self.normalizeOnce.Do(func() {
		if self.Handshaker == nil {
			self.Handshaker = defaultHandshaker{}
		}

		if self.Keepaliver == nil {
			self.Keepaliver = defaultKeepaliver{}
		}

		if self.Stream == nil {
			self.Stream = &defaultStreamOptions
		}

		self.logger = self.Stream.Normalize().Logger()
	})

	return self
}

func (self *Options) SetRequestFactory(serviceName string, methodName string, requestFactory MessageFactory) *Options {
	self.serviceOptionsManager.SetRequestFactory(serviceName, methodName, requestFactory)
	return self
}

func (self *Options) SetResponseFactory(serviceName string, methodName string, responseFactory MessageFactory) *Options {
	self.serviceOptionsManager.SetResponseFactory(serviceName, methodName, responseFactory)
	return self
}

func (self *Options) SetIncomingRPCHandler(serviceName string, methodName string, rpcHandler RPCHandler) *Options {
	self.serviceOptionsManager.SetIncomingRPCHandler(serviceName, methodName, rpcHandler)
	return self
}

func (self *Options) AddIncomingRPCInterceptor(serviceName string, methodName string, rpcInterceptor RPCHandler) *Options {
	self.serviceOptionsManager.AddIncomingRPCInterceptor(serviceName, methodName, rpcInterceptor)
	return self
}

func (self *Options) AddOutgoingRPCInterceptor(serviceName string, methodName string, rpcInterceptor RPCHandler) *Options {
	self.serviceOptionsManager.AddOutgoingRPCInterceptor(serviceName, methodName, rpcInterceptor)
	return self
}

func (self *Options) Logger() *zerolog.Logger {
	return self.logger
}

type Keepaliver interface {
	NewKeepalive() (keepalive stream.Message)
	HandleKeepalive(ctx context.Context, keepalive stream.Message) (err error)
	EmitKeepalive() (keepalive stream.Message, err error)
}

type ServiceOptions struct {
	Methods map[string]*MethodOptions

	incomingRPCInterceptors []RPCHandler
	outgoingRPCInterceptors []RPCHandler
}

type MethodOptions struct {
	RequestFactory          MessageFactory
	ResponseFactory         MessageFactory
	IncomingRPCHandler      RPCHandler
	IncomingRPCInterceptors []RPCHandler
	OutgoingRPCInterceptors []RPCHandler
}

type MessageFactory func() (message stream.Message)

type defaultHandshaker struct{}

func (defaultHandshaker) NewHandshake() stream.Message {
	return stream.NullMessage
}

func (defaultHandshaker) HandleHandshake(context.Context, stream.Message) (bool, error) {
	return true, nil
}

func (defaultHandshaker) EmitHandshake() (stream.Message, error) {
	return stream.NullMessage, nil
}

type defaultKeepaliver struct{}

func (defaultKeepaliver) NewKeepalive() stream.Message {
	return stream.NullMessage
}

func (defaultKeepaliver) HandleKeepalive(context.Context, stream.Message) error {
	return nil
}

func (defaultKeepaliver) EmitKeepalive() (stream.Message, error) {
	return stream.NullMessage, nil
}

type serviceOptionsManager struct {
	Services map[string]*ServiceOptions

	incomingRPCInterceptors []RPCHandler
	outgoingRPCInterceptors []RPCHandler
}

func (self *serviceOptionsManager) SetRequestFactory(serviceName string, methodName string, messageFactory MessageFactory) {
	method := self.getOrSetService(serviceName).getOrSetMethod(methodName)
	method.RequestFactory = messageFactory
}

func (self *serviceOptionsManager) SetResponseFactory(serviceName string, methodName string, messageFactory MessageFactory) {
	method := self.getOrSetService(serviceName).getOrSetMethod(methodName)
	method.ResponseFactory = messageFactory
}

func (self *serviceOptionsManager) SetIncomingRPCHandler(serviceName string, methodName string, rpcHandler RPCHandler) {
	method := self.getOrSetService(serviceName).getOrSetMethod(methodName)
	method.IncomingRPCHandler = rpcHandler
}

func (self *serviceOptionsManager) AddIncomingRPCInterceptor(serviceName string, methodName string, rpcInterceptor RPCHandler) {
	i := len(self.incomingRPCInterceptors)

	if serviceName == "" {
		self.incomingRPCInterceptors = append(self.incomingRPCInterceptors, rpcInterceptor)

		for _, service := range self.Services {
			for _, method := range service.Methods {
				rpcInterceptors := make([]RPCHandler, len(method.IncomingRPCInterceptors)+1)
				copy(rpcInterceptors[:i], method.IncomingRPCInterceptors[:i])
				rpcInterceptors[i] = rpcInterceptor
				copy(rpcInterceptors[i+1:], method.IncomingRPCInterceptors[i:])
				method.IncomingRPCInterceptors = rpcInterceptors
			}
		}
	} else {
		service := self.getOrSetService(serviceName)
		j := i + len(service.incomingRPCInterceptors)

		if methodName == "" {
			service.incomingRPCInterceptors = append(service.incomingRPCInterceptors, rpcInterceptor)

			for _, method := range service.Methods {
				rpcInterceptors := make([]RPCHandler, len(method.IncomingRPCInterceptors)+1)
				copy(rpcInterceptors[:j], method.IncomingRPCInterceptors[:j])
				rpcInterceptors[j] = rpcInterceptor
				copy(rpcInterceptors[j+1:], method.IncomingRPCInterceptors[j:])
				method.IncomingRPCInterceptors = rpcInterceptors
			}
		} else {
			method := service.getOrSetMethod(methodName)
			k := j + len(method.IncomingRPCInterceptors)
			rpcInterceptors := make([]RPCHandler, k+1)
			copy(rpcInterceptors, self.incomingRPCInterceptors)
			copy(rpcInterceptors[i:], service.incomingRPCInterceptors)
			copy(rpcInterceptors[j:], method.IncomingRPCInterceptors)
			rpcInterceptors[k] = rpcInterceptor
			method.IncomingRPCInterceptors = rpcInterceptors
		}
	}
}

func (self *serviceOptionsManager) AddOutgoingRPCInterceptor(serviceName string, methodName string, rpcInterceptor RPCHandler) {
	i := len(self.outgoingRPCInterceptors)

	if serviceName == "" {
		self.outgoingRPCInterceptors = append(self.outgoingRPCInterceptors, rpcInterceptor)

		for _, service := range self.Services {
			for _, method := range service.Methods {
				rpcInterceptors := make([]RPCHandler, len(method.OutgoingRPCInterceptors)+1)
				copy(rpcInterceptors[:i], method.OutgoingRPCInterceptors[:i])
				rpcInterceptors[i] = rpcInterceptor
				copy(rpcInterceptors[i+1:], method.OutgoingRPCInterceptors[i:])
				method.OutgoingRPCInterceptors = rpcInterceptors
			}
		}
	} else {
		service := self.getOrSetService(serviceName)
		j := i + len(service.outgoingRPCInterceptors)

		if methodName == "" {
			service.outgoingRPCInterceptors = append(service.outgoingRPCInterceptors, rpcInterceptor)

			for _, method := range service.Methods {
				rpcInterceptors := make([]RPCHandler, len(method.OutgoingRPCInterceptors)+1)
				copy(rpcInterceptors[:j], method.OutgoingRPCInterceptors[:j])
				rpcInterceptors[j] = rpcInterceptor
				copy(rpcInterceptors[j+1:], method.OutgoingRPCInterceptors[j:])
				method.OutgoingRPCInterceptors = rpcInterceptors
			}
		} else {
			method := service.getOrSetMethod(methodName)
			k := j + len(method.OutgoingRPCInterceptors)
			rpcInterceptors := make([]RPCHandler, k+1)
			copy(rpcInterceptors, self.outgoingRPCInterceptors)
			copy(rpcInterceptors[i:], service.outgoingRPCInterceptors)
			copy(rpcInterceptors[j:], method.OutgoingRPCInterceptors)
			rpcInterceptors[k] = rpcInterceptor
			method.OutgoingRPCInterceptors = rpcInterceptors
		}
	}
}

func (self *serviceOptionsManager) GetMethod(serviceName string, methodName string) *MethodOptions {
	service, ok := self.Services[serviceName]

	if !ok {
		return &defaultMethodOptions
	}

	method, ok := service.Methods[methodName]

	if !ok {
		return &defaultMethodOptions
	}

	return method
}

func (self *serviceOptionsManager) getOrSetService(serviceName string) *ServiceOptions {
	utils.Assert(serviceName != "", func() string { return "pbrpc/channel: empty service name" })
	services := self.Services

	if services == nil {
		services = map[string]*ServiceOptions{}
		self.Services = services
	}

	service := services[serviceName]

	if service == nil {
		service = new(ServiceOptions)
		services[serviceName] = service
	}

	return service
}

func (self *ServiceOptions) getOrSetMethod(methodName string) *MethodOptions {
	utils.Assert(methodName != "", func() string { return "pbrpc/channel: empty method name" })
	methods := self.Methods

	if methods == nil {
		methods = map[string]*MethodOptions{}
		self.Methods = methods
	}

	method := methods[methodName]

	if method == nil {
		method = new(MethodOptions)
		*method = defaultMethodOptions
		methods[methodName] = method
	}

	return method
}

var defaultStreamOptions stream.Options

var defaultMethodOptions = MethodOptions{
	RequestFactory:  defaultRequestFactory,
	ResponseFactory: defaultResponseFactory,
}

func defaultRequestFactory() stream.Message  { return stream.NullMessage }
func defaultResponseFactory() stream.Message { return stream.NullMessage }
