package pbrpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type Server struct {
	policy           *ServerPolicy
	bindAddress      string
	discoveryAddress string
	openness         int32
}

func (self *Server) Initialize(policy *ServerPolicy, bindAddress string, discoveryAddress string) *Server {
	if self.openness != 0 {
		panic(errors.New("pbrpc: server already initialized"))
	}

	self.policy = policy.Validate()

	if bindAddress == "" {
		bindAddress = defaultServerAddress
	}

	self.bindAddress = bindAddress
	self.discoveryAddress = discoveryAddress
	self.openness = 1
	return self
}

func (self *Server) Run(context_ context.Context) error {
	if self.openness != 1 {
		return nil
	}

	acceptorEventHandler := AcceptorEventHandler{
		OnConnect: func(context_ context.Context, connection net.Conn) {
			channel, e := self.policy.ChannelFactory.CreateProduct(self.policy.Channel, connection)
			logger_ := &self.policy.Channel.Logger

			if e != nil {
				logger_.Errorf("channel creation failure: clientAddress=%q, e=%q", connection.RemoteAddr(), e)
				connection.Close()
				return
			}

			e = channel.Run(context_)
			logger_.Infof("channel run-out: clientAddress=%q, e=%q", connection.RemoteAddr(), e)
			self.policy.ChannelFactory.DestroyProduct(channel)
		},
	}

	if self.policy.Registry != nil {
		serviceNames := []string(nil)

		for serviceName := range self.policy.Channel.serviceHandlers {
			serviceNames = append(serviceNames, serviceName)
		}

		address := self.discoveryAddress

		acceptorEventHandler.OnListen = func(context_ context.Context, listener net.Listener) error {
			if address == "" {
				address = listener.Addr().String()
			}

			return self.policy.Registry.AddServiceProviders(context_, serviceNames, address, self.policy.Weight)
		}

		acceptorEventHandler.OnClose = func() {
			self.policy.Registry.RemoveServiceProviders(context.Background(), serviceNames, address, self.policy.Weight)
		}
	}

	e := self.policy.Acceptor.Accept(context_, self.bindAddress, self.policy.GracefulShutdownTimeout, acceptorEventHandler)
	self.policy = nil
	self.bindAddress = ""
	self.discoveryAddress = ""
	self.openness = -1
	return e
}

type ServerPolicy struct {
	Acceptor                Acceptor
	GracefulShutdownTimeout time.Duration
	Registry                *Registry
	Weight                  int32
	ChannelFactory          ServerChannelFactory
	Channel                 *ServerChannelPolicy

	validateOnce sync.Once
}

func (self *ServerPolicy) Validate() *ServerPolicy {
	self.validateOnce.Do(func() {
		if self.Acceptor == nil {
			self.Acceptor = TCPAcceptor{}
		}

		if self.Registry != nil {
			if self.Weight < 1 {
				self.Weight = defaultWeight
			}
		}

		if self.ChannelFactory == nil {
			self.ChannelFactory = defaultServerChannelFactory{}
		}

		if self.Channel == nil {
			self.Channel = &defaultServerChannelPolicy
		}
	})

	return self
}

type ServerChannelFactory interface {
	CreateProduct(*ServerChannelPolicy, net.Conn) (*ServerChannel, error)
	DestroyProduct(*ServerChannel)
}

const defaultWeight = 5

type defaultServerChannelFactory struct{}

func (defaultServerChannelFactory) CreateProduct(productPolicy *ServerChannelPolicy, connection net.Conn) (*ServerChannel, error) {
	return (&ServerChannel{}).Initialize(productPolicy, connection), nil
}

func (defaultServerChannelFactory) DestroyProduct(_ *ServerChannel) {
}

var defaultServerChannelPolicy ServerChannelPolicy
