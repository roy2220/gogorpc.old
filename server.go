package pbrpc

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/let-z-go/toolkit/logger"
)

type Server struct {
	policy           *ServerPolicy
	bindAddress      string
	discoveryAddress string
	context1         context.Context
	context2         context.Context
	stop1            context.CancelFunc
	stop2            context.CancelFunc
	openness         int32
}

func (self *Server) Initialize(policy *ServerPolicy, bindAddress string, discoveryAddress string, context_ context.Context) *Server {
	if self.openness != 0 {
		panic(errors.New("pbrpc: server already initialized"))
	}

	self.policy = policy.Validate()

	if bindAddress == "" {
		bindAddress = defaultServerAddress
	}

	self.bindAddress = bindAddress
	self.discoveryAddress = discoveryAddress
	self.context1, self.stop1 = context.WithCancel(context_)
	self.context2, self.stop2 = context.WithCancel(self.context1)
	self.openness = 1
	return self
}

func (self *Server) Run() error {
	if self.openness != 1 {
		return nil
	}

	cleanup := func() {
		self.policy = nil
		self.bindAddress = ""
		self.discoveryAddress = ""
		self.openness = -1
	}

	var serviceNames []string
	var address string

	if self.policy.Registry != nil {
		serviceNames = nil

		for serviceName := range self.policy.Channel.serviceHandlers {
			serviceNames = append(serviceNames, serviceName)
		}

		address = self.discoveryAddress

		if address == "" {
			address = self.bindAddress
		}

		if e := self.policy.Registry.AddServiceProviders(serviceNames, address, self.policy.Weight); e != nil {
			cleanup()
			return e
		}
	}

	connectionHandler := func(
		policy *ServerPolicy,
		context_ context.Context,
		logger_ *logger.Logger,
	) func(connection net.Conn) {
		return func(connection net.Conn) {
			channel, e := policy.ChannelFactory.CreateProduct(policy.Channel, connection, context_)

			if e != nil {
				logger_.Errorf("channel creation failure: clientAddress=%#v, e=%#v", connection.RemoteAddr().String(), e.Error())
				connection.Close()
				return
			}

			e = channel.Run()
			logger_.Infof("channel run-out: clientAddress=%#v, e=%#v", connection.RemoteAddr().String(), e.Error())
			policy.ChannelFactory.DestroyProduct(channel)
		}
	}(

		self.policy,
		self.context1,
		&self.policy.Channel.Logger,
	)

	e := self.policy.Acceptor.Accept(self.context2, self.bindAddress, connectionHandler)

	if self.policy.Registry != nil {
		self.policy.Registry.RemoveServiceProviders(serviceNames, address, self.policy.Weight)
	}

	cleanup()
	return e
}

func (self *Server) Stop(force bool) {
	var stop context.CancelFunc

	if force {
		stop = self.stop1
	} else {
		stop = self.stop2
	}

	if stop != nil {
		stop()
	}
}

type ServerPolicy struct {
	Acceptor       Acceptor
	Registry       *Registry
	Weight         int32
	ChannelFactory ServerChannelFactory
	Channel        *ServerChannelPolicy

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
	CreateProduct(*ServerChannelPolicy, net.Conn, context.Context) (*ServerChannel, error)
	DestroyProduct(*ServerChannel)
}

const defaultWeight = 5

type defaultServerChannelFactory struct{}

func (defaultServerChannelFactory) CreateProduct(productPolicy *ServerChannelPolicy, connection net.Conn, context_ context.Context) (*ServerChannel, error) {
	return (&ServerChannel{}).Initialize(productPolicy, connection, context_), nil
}

func (defaultServerChannelFactory) DestroyProduct(_ *ServerChannel) {
}

var defaultServerChannelPolicy ServerChannelPolicy
