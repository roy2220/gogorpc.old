package pbrpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Server struct {
	policy           *ServerPolicy
	bindAddress      string
	discoveryAddress string
	id               int32
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
	self.id = -1
	self.openness = 1
	return self
}

func (self *Server) Run(context_ context.Context) error {
	if self.openness != 1 {
		return nil
	}

	acceptorEventHandler := AcceptorEventHandler{
		OnListen: func(_ context.Context, listener net.Listener) error {
			self.policy.Channel.Logger.Infof("listening on %s: serverID=%#v", listener.Addr(), self.id)
			return nil
		},

		OnConnect: func(context_ context.Context, connection net.Conn) {
			channel, e := self.policy.ChannelFactory.CreateProduct(self.policy.Channel, self.id, connection)
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

		OnClose: func() {
			self.policy.Channel.Logger.Infof("closing: serverID=%d", self.id)
		},
	}

	if self.policy.Registry != nil {
		onListen, onClose := acceptorEventHandler.OnListen, acceptorEventHandler.OnClose
		serviceNames := make([]string, len(self.policy.Channel.serviceHandlers))

		for i := range serviceNames {
			serviceNames[i] = self.policy.Channel.serviceHandlers[i].key
		}

		address := self.discoveryAddress

		acceptorEventHandler.OnListen = func(context_ context.Context, listener net.Listener) error {
			if address == "" {
				address = listener.Addr().String()
			}

			serverID, e := self.policy.Registry.registerServer(context_, &serverInfo{
				Address:              address,
				Weight:               self.policy.Weight,
				ProvidedServiceNames: serviceNames,
			})

			if e != nil {
				return e
			}

			atomic.StoreInt32(&self.id, serverID)
			onListen(context_, listener)
			return nil
		}

		acceptorEventHandler.OnClose = func() {
			onClose()
			self.policy.Registry.unregisterServer(context.Background(), self.id)
		}
	}

	e := self.policy.Acceptor.Accept(context_, self.bindAddress, self.policy.GracefulShutdownTimeout, acceptorEventHandler)
	self.policy = nil
	self.bindAddress = ""
	self.discoveryAddress = ""
	self.openness = -1
	return e
}

func (self *Server) GetID() int32 {
	return atomic.LoadInt32(&self.id)
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
	CreateProduct(productPolicy *ServerChannelPolicy, creatorID int32, connection net.Conn) (product *ServerChannel, e error)
	DestroyProduct(product *ServerChannel)
}

const defaultWeight = 5

type defaultServerChannelFactory struct{}

func (defaultServerChannelFactory) CreateProduct(productPolicy *ServerChannelPolicy, creatorID int32, connection net.Conn) (*ServerChannel, error) {
	return (&ServerChannel{}).Initialize(productPolicy, creatorID, connection), nil
}

func (defaultServerChannelFactory) DestroyProduct(_ *ServerChannel) {
}

var defaultServerChannelPolicy ServerChannelPolicy
