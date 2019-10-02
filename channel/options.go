package channel

import (
	"sync"

	"github.com/rs/zerolog"
)

type Options struct {
	Stream           *StreamOptions
	Logger           *zerolog.Logger
	ExtensionFactory ExtensionFactory

	serviceOptionsManager

	normalizeOnce sync.Once
}

func (self *Options) Normalize() *Options {
	self.normalizeOnce.Do(func() {
		if self.Stream == nil {
			self.Stream = &defaultStreamOptions
		}

		self.Stream.Normalize()

		if self.Logger == nil {
			self.Logger = self.Stream.Logger
		}

		if self.ExtensionFactory == nil {
			self.ExtensionFactory = func(bool) Extension {
				return DummyExtension{}
			}
		}

		if !self.GeneralMethod.requestFactoryIsSet {
			self.setRequestFactory("", "", GetNullMessage)
		}
	})

	return self
}

func (self *Options) BuildMethod(serviceID string, methodName string) MethodOptionsBuilder {
	return MethodOptionsBuilder{self, serviceID, methodName}
}

func (self *Options) Do(doer func(*Options)) *Options {
	doer(self)
	return self
}

type ExtensionFactory func(extensionIsServerSide bool) (extension Extension)

type MethodOptionsBuilder struct {
	options *Options

	serviceID  string
	methodName string
}

func (self MethodOptionsBuilder) SetRequestFactory(requestFactory MessageFactory) MethodOptionsBuilder {
	self.options.setRequestFactory(self.serviceID, self.methodName, requestFactory)
	return self
}

func (self MethodOptionsBuilder) SetIncomingRPCHandler(rpcHandler RPCHandler) MethodOptionsBuilder {
	self.options.setIncomingRPCHandler(self.serviceID, self.methodName, rpcHandler)
	return self
}

func (self MethodOptionsBuilder) AddIncomingRPCInterceptor(rpcInterceptor RPCHandler) MethodOptionsBuilder {
	self.options.addIncomingRPCInterceptor(self.serviceID, self.methodName, rpcInterceptor)
	return self
}

func (self MethodOptionsBuilder) AddOutgoingRPCInterceptor(rpcInterceptor RPCHandler) MethodOptionsBuilder {
	self.options.addOutgoingRPCInterceptor(self.serviceID, self.methodName, rpcInterceptor)
	return self
}

func (self MethodOptionsBuilder) End() *Options {
	return self.options
}

type ServiceOptions struct {
	GeneralMethod MethodOptions
	Methods       map[string]*MethodOptions
}

type MethodOptions struct {
	RequestFactory          MessageFactory
	IncomingRPCHandler      RPCHandler
	IncomingRPCInterceptors []RPCHandler
	OutgoingRPCInterceptors []RPCHandler

	requestFactoryIsSet     bool
	incomingRPCHandlerIsSet bool
}

type MessageFactory func() (message Message)

func GetNullMessage() Message {
	return NullMessage
}

var _ = MessageFactory(GetNullMessage)

func NewRawMessage() Message {
	return new(RawMessage)
}

var _ = MessageFactory(NewRawMessage)

type serviceOptionsManager struct {
	GeneralMethod MethodOptions
	Services      map[string]*ServiceOptions
}

func (self *serviceOptionsManager) GetMethod(serviceID string, methodName string) *MethodOptions {
	service, ok := self.Services[serviceID]

	if !ok {
		return &self.GeneralMethod
	}

	method, ok := service.Methods[methodName]

	if !ok {
		return &service.GeneralMethod
	}

	return method
}

func (self *serviceOptionsManager) setRequestFactory(serviceID string, methodName string, messageFactory MessageFactory) {
	if serviceID == "" {
		self.GeneralMethod.RequestFactory = messageFactory
		self.GeneralMethod.requestFactoryIsSet = true

		for _, service := range self.Services {
			if service.GeneralMethod.requestFactoryIsSet {
				continue
			}

			service.GeneralMethod.RequestFactory = messageFactory

			for _, method := range service.Methods {
				if method.requestFactoryIsSet {
					continue
				}

				method.RequestFactory = messageFactory
			}
		}
	} else {
		service := self.getOrSetService(serviceID)

		if methodName == "" {
			service.GeneralMethod.RequestFactory = messageFactory
			service.GeneralMethod.requestFactoryIsSet = true

			for _, method := range service.Methods {
				if method.requestFactoryIsSet {
					continue
				}

				method.RequestFactory = messageFactory
			}
		} else {
			method := service.getOrSetMethod(methodName)
			method.RequestFactory = messageFactory
			method.requestFactoryIsSet = true
		}
	}
}

func (self *serviceOptionsManager) setIncomingRPCHandler(serviceID string, methodName string, rpcHandler RPCHandler) {
	if serviceID == "" {
		self.GeneralMethod.IncomingRPCHandler = rpcHandler
		self.GeneralMethod.incomingRPCHandlerIsSet = true

		for _, service := range self.Services {
			if service.GeneralMethod.incomingRPCHandlerIsSet {
				continue
			}

			service.GeneralMethod.IncomingRPCHandler = rpcHandler

			for _, method := range service.Methods {
				if method.incomingRPCHandlerIsSet {
					continue
				}

				method.IncomingRPCHandler = rpcHandler
			}
		}
	} else {
		service := self.getOrSetService(serviceID)

		if methodName == "" {
			service.GeneralMethod.IncomingRPCHandler = rpcHandler
			service.GeneralMethod.incomingRPCHandlerIsSet = true

			for _, method := range service.Methods {
				if method.incomingRPCHandlerIsSet {
					continue
				}

				method.IncomingRPCHandler = rpcHandler
			}
		} else {
			method := service.getOrSetMethod(methodName)
			method.IncomingRPCHandler = rpcHandler
			method.incomingRPCHandlerIsSet = true
		}
	}
}

func (self *serviceOptionsManager) addIncomingRPCInterceptor(serviceID string, methodName string, rpcInterceptor RPCHandler) {
	if serviceID == "" {
		i := len(self.GeneralMethod.IncomingRPCInterceptors)
		insertRPCInterceptor(rpcInterceptor, &self.GeneralMethod.IncomingRPCInterceptors, i)

		for _, service := range self.Services {
			insertRPCInterceptor(rpcInterceptor, &service.GeneralMethod.IncomingRPCInterceptors, i)

			for _, method := range service.Methods {
				insertRPCInterceptor(rpcInterceptor, &method.IncomingRPCInterceptors, i)
			}
		}
	} else {
		service := self.getOrSetService(serviceID)

		if methodName == "" {
			i := len(service.GeneralMethod.IncomingRPCInterceptors)
			insertRPCInterceptor(rpcInterceptor, &service.GeneralMethod.IncomingRPCInterceptors, i)

			for _, method := range service.Methods {
				insertRPCInterceptor(rpcInterceptor, &method.IncomingRPCInterceptors, i)
			}
		} else {
			method := service.getOrSetMethod(methodName)
			insertRPCInterceptor(rpcInterceptor, &method.IncomingRPCInterceptors, len(method.IncomingRPCInterceptors))
		}
	}
}

func (self *serviceOptionsManager) addOutgoingRPCInterceptor(serviceID string, methodName string, rpcInterceptor RPCHandler) {
	if serviceID == "" {
		i := len(self.GeneralMethod.OutgoingRPCInterceptors)
		insertRPCInterceptor(rpcInterceptor, &self.GeneralMethod.OutgoingRPCInterceptors, i)

		for _, service := range self.Services {
			insertRPCInterceptor(rpcInterceptor, &service.GeneralMethod.OutgoingRPCInterceptors, i)

			for _, method := range service.Methods {
				insertRPCInterceptor(rpcInterceptor, &method.OutgoingRPCInterceptors, i)
			}
		}
	} else {
		service := self.getOrSetService(serviceID)

		if methodName == "" {
			i := len(service.GeneralMethod.OutgoingRPCInterceptors)
			insertRPCInterceptor(rpcInterceptor, &service.GeneralMethod.OutgoingRPCInterceptors, i)

			for _, method := range service.Methods {
				insertRPCInterceptor(rpcInterceptor, &method.OutgoingRPCInterceptors, i)
			}
		} else {
			method := service.getOrSetMethod(methodName)
			insertRPCInterceptor(rpcInterceptor, &method.OutgoingRPCInterceptors, len(method.OutgoingRPCInterceptors))
		}
	}
}

func (self *serviceOptionsManager) getOrSetService(serviceID string) *ServiceOptions {
	services := self.Services

	if services == nil {
		services = map[string]*ServiceOptions{}
		self.Services = services
	}

	service := services[serviceID]

	if service == nil {
		service = new(ServiceOptions)
		service.GeneralMethod.RequestFactory = self.GeneralMethod.RequestFactory
		service.GeneralMethod.IncomingRPCHandler = self.GeneralMethod.IncomingRPCHandler
		service.GeneralMethod.IncomingRPCInterceptors = copyRPCInterceptors(self.GeneralMethod.IncomingRPCInterceptors)
		service.GeneralMethod.OutgoingRPCInterceptors = copyRPCInterceptors(self.GeneralMethod.OutgoingRPCInterceptors)
		services[serviceID] = service
	}

	return service
}

func (self *ServiceOptions) getOrSetMethod(methodName string) *MethodOptions {
	methods := self.Methods

	if methods == nil {
		methods = map[string]*MethodOptions{}
		self.Methods = methods
	}

	method := methods[methodName]

	if method == nil {
		method = new(MethodOptions)
		method.RequestFactory = self.GeneralMethod.RequestFactory
		method.IncomingRPCHandler = self.GeneralMethod.IncomingRPCHandler
		method.IncomingRPCInterceptors = copyRPCInterceptors(self.GeneralMethod.IncomingRPCInterceptors)
		method.OutgoingRPCInterceptors = copyRPCInterceptors(self.GeneralMethod.OutgoingRPCInterceptors)
		methods[methodName] = method
	}

	return method
}

var defaultStreamOptions StreamOptions

func copyRPCInterceptors(rpcInterceptors []RPCHandler) []RPCHandler {
	rpcInterceptorsCopy := make([]RPCHandler, len(rpcInterceptors))
	copy(rpcInterceptorsCopy, rpcInterceptors)
	return rpcInterceptorsCopy
}

func insertRPCInterceptor(rpcInterceptor RPCHandler, rpcInterceptors *[]RPCHandler, i int) {
	newRPCInterceptors := make([]RPCHandler, len(*rpcInterceptors)+1)
	copy(newRPCInterceptors[:i], (*rpcInterceptors)[:i])
	newRPCInterceptors[i] = rpcInterceptor
	copy(newRPCInterceptors[i+1:], (*rpcInterceptors)[i:])
	*rpcInterceptors = newRPCInterceptors
}
