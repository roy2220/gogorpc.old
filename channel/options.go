package channel

import (
	"fmt"
	"sync"

	"github.com/let-z-go/toolkit/utils"
	"github.com/rs/zerolog"
)

type Options struct {
	Stream           *StreamOptions
	Logger           *zerolog.Logger
	ExtensionFactory ExtensionFactory

	serviceOptionsManager

	normalizeOnce sync.Once
}

func (o *Options) Normalize() *Options {
	o.normalizeOnce.Do(func() {
		if o.Stream == nil {
			o.Stream = &defaultStreamOptions
		}

		o.Stream.Normalize()

		if o.Logger == nil {
			o.Logger = o.Stream.Logger
		}

		if o.ExtensionFactory == nil {
			o.ExtensionFactory = DummyExtensionFactory
		}

		if !o.GeneralMethod.requestFactoryIsSet {
			o.setRequestFactory("", "", GetNullMessage)
		}
	})

	return o
}

func (o *Options) BuildMethod(serviceName string, methodName string) MethodOptionsBuilder {
	return MethodOptionsBuilder{o, serviceName, methodName}
}

func (o *Options) Do(doer func(*Options)) *Options {
	doer(o)
	return o
}

type MethodOptionsBuilder struct {
	options *Options

	serviceName string
	methodName  string
}

func (mob MethodOptionsBuilder) SetRequestFactory(requestFactory MessageFactory) MethodOptionsBuilder {
	mob.options.setRequestFactory(mob.serviceName, mob.methodName, requestFactory)
	return mob
}

func (mob MethodOptionsBuilder) SetIncomingRPCHandler(rpcHandler RPCHandler) MethodOptionsBuilder {
	mob.options.setIncomingRPCHandler(mob.serviceName, mob.methodName, rpcHandler)
	return mob
}

func (mob MethodOptionsBuilder) AddIncomingRPCInterceptor(rpcInterceptor RPCHandler) MethodOptionsBuilder {
	mob.options.addIncomingRPCInterceptor(mob.serviceName, mob.methodName, rpcInterceptor)
	return mob
}

func (mob MethodOptionsBuilder) AddOutgoingRPCInterceptor(rpcInterceptor RPCHandler) MethodOptionsBuilder {
	mob.options.addOutgoingRPCInterceptor(mob.serviceName, mob.methodName, rpcInterceptor)
	return mob
}

func (mob MethodOptionsBuilder) End() *Options {
	return mob.options
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

func (som *serviceOptionsManager) GetMethod(serviceName string, methodName string) *MethodOptions {
	service, ok := som.Services[serviceName]

	if !ok {
		return &som.GeneralMethod
	}

	method, ok := service.Methods[methodName]

	if !ok {
		return &service.GeneralMethod
	}

	return method
}

func (som *serviceOptionsManager) setRequestFactory(serviceName string, methodName string, messageFactory MessageFactory) {
	if serviceName == "" {
		utils.Assert(methodName == "", func() string {
			return fmt.Sprintf("gogorpc/channel: invalid argument: methodName=%#v, serviceName=%#v", methodName, serviceName)
		})

		som.GeneralMethod.RequestFactory = messageFactory
		som.GeneralMethod.requestFactoryIsSet = true

		for _, service := range som.Services {
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
		service := som.getOrSetService(serviceName)

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

func (som *serviceOptionsManager) setIncomingRPCHandler(serviceName string, methodName string, rpcHandler RPCHandler) {
	if serviceName == "" {
		utils.Assert(methodName == "", func() string {
			return fmt.Sprintf("gogorpc/channel: invalid argument: methodName=%#v, serviceName=%#v", methodName, serviceName)
		})

		som.GeneralMethod.IncomingRPCHandler = rpcHandler
		som.GeneralMethod.incomingRPCHandlerIsSet = true

		for _, service := range som.Services {
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
		service := som.getOrSetService(serviceName)

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

func (som *serviceOptionsManager) addIncomingRPCInterceptor(serviceName string, methodName string, rpcInterceptor RPCHandler) {
	if serviceName == "" {
		utils.Assert(methodName == "", func() string {
			return fmt.Sprintf("gogorpc/channel: invalid argument: methodName=%#v, serviceName=%#v", methodName, serviceName)
		})

		i := len(som.GeneralMethod.IncomingRPCInterceptors)
		insertRPCInterceptor(rpcInterceptor, &som.GeneralMethod.IncomingRPCInterceptors, i)

		for _, service := range som.Services {
			insertRPCInterceptor(rpcInterceptor, &service.GeneralMethod.IncomingRPCInterceptors, i)

			for _, method := range service.Methods {
				insertRPCInterceptor(rpcInterceptor, &method.IncomingRPCInterceptors, i)
			}
		}
	} else {
		service := som.getOrSetService(serviceName)

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

func (som *serviceOptionsManager) addOutgoingRPCInterceptor(serviceName string, methodName string, rpcInterceptor RPCHandler) {
	if serviceName == "" {
		utils.Assert(methodName == "", func() string {
			return fmt.Sprintf("gogorpc/channel: invalid argument: methodName=%#v, serviceName=%#v", methodName, serviceName)
		})

		i := len(som.GeneralMethod.OutgoingRPCInterceptors)
		insertRPCInterceptor(rpcInterceptor, &som.GeneralMethod.OutgoingRPCInterceptors, i)

		for _, service := range som.Services {
			insertRPCInterceptor(rpcInterceptor, &service.GeneralMethod.OutgoingRPCInterceptors, i)

			for _, method := range service.Methods {
				insertRPCInterceptor(rpcInterceptor, &method.OutgoingRPCInterceptors, i)
			}
		}
	} else {
		service := som.getOrSetService(serviceName)

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

func (som *serviceOptionsManager) getOrSetService(serviceName string) *ServiceOptions {
	services := som.Services

	if services == nil {
		services = map[string]*ServiceOptions{}
		som.Services = services
	}

	service := services[serviceName]

	if service == nil {
		service = new(ServiceOptions)
		service.GeneralMethod.RequestFactory = som.GeneralMethod.RequestFactory
		service.GeneralMethod.IncomingRPCHandler = som.GeneralMethod.IncomingRPCHandler
		service.GeneralMethod.IncomingRPCInterceptors = copyRPCInterceptors(som.GeneralMethod.IncomingRPCInterceptors)
		service.GeneralMethod.OutgoingRPCInterceptors = copyRPCInterceptors(som.GeneralMethod.OutgoingRPCInterceptors)
		services[serviceName] = service
	}

	return service
}

func (so *ServiceOptions) getOrSetMethod(methodName string) *MethodOptions {
	methods := so.Methods

	if methods == nil {
		methods = map[string]*MethodOptions{}
		so.Methods = methods
	}

	method := methods[methodName]

	if method == nil {
		method = new(MethodOptions)
		method.RequestFactory = so.GeneralMethod.RequestFactory
		method.IncomingRPCHandler = so.GeneralMethod.IncomingRPCHandler
		method.IncomingRPCInterceptors = copyRPCInterceptors(so.GeneralMethod.IncomingRPCInterceptors)
		method.OutgoingRPCInterceptors = copyRPCInterceptors(so.GeneralMethod.OutgoingRPCInterceptors)
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
	*rpcInterceptors = append(*rpcInterceptors, nil)

	for j := len(*rpcInterceptors) - 1; j > i; j-- {
		(*rpcInterceptors)[j] = (*rpcInterceptors)[j-1]
	}

	(*rpcInterceptors)[i] = rpcInterceptor
}
