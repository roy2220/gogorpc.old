package pbrpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

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

	if context_ == nil {
		context_ = context.Background()
	}

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

	var listener *net.TCPListener

	if listener2, e := net.Listen("tcp", self.bindAddress); e == nil {
		listener = listener2.(*net.TCPListener)
	} else {
		cleanup()
		return e
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
			address = listener.Addr().String()
		}

		if e := self.policy.Registry.AddServiceProviders(serviceNames, address, self.policy.Weight); e != nil {
			listener.Close()
			cleanup()
			return e
		}
	}

	var wg sync.WaitGroup
	var e error

	for {
		var deadline time.Time
		deadline, e = makeDeadline(self.context2, acceptTimeoutOfServer)

		if e != nil {
			break
		}

		e = listener.SetDeadline(deadline)

		if e != nil {
			break
		}

		var connection net.Conn
		connection, e = listener.Accept()

		if e != nil {
			if e, ok := e.(*net.OpError); ok && e.Timeout() {
				continue
			}

			break
		}

		wg.Add(1)

		go func(
			policy *ServerPolicy,
			connection net.Conn,
			context_ context.Context,
			logger_ *logger.Logger,
		) {
			channel, e := policy.ChannelFactory.CreateProduct(&policy.Channel, connection, context_)

			if e != nil {
				logger_.Errorf("channel creation failure: clientAddress=%#v, e=%#v", connection.RemoteAddr().String(), e.Error())
				connection.Close()
				wg.Done()
				return
			}

			e = channel.Run()
			logger_.Infof("channel run-out: clientAddress=%#v, e=%#v", connection.RemoteAddr().String(), e.Error())
			policy.ChannelFactory.DestroyProduct(channel)
			wg.Done()
		}(

			self.policy,
			connection,
			self.context1,
			&self.policy.Channel.Logger,
		)
	}

	if self.policy.Registry != nil {
		self.policy.Registry.RemoveServiceProviders(serviceNames, address, self.policy.Weight)
	}

	listener.Close()
	cleanup()
	wg.Wait()
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
	Registry       *Registry
	Weight         int32
	ChannelFactory ServerChannelFactory
	Channel        ChannelPolicy
	validateOnce   sync.Once
}

func (self *ServerPolicy) RegisterServiceHandler(serviceHandler ServiceHandler) *ServerPolicy {
	self.Channel.RegisterServiceHandler(serviceHandler)
	return self
}

func (self *ServerPolicy) Validate() *ServerPolicy {
	self.validateOnce.Do(func() {
		if self.Registry != nil {
			if self.Weight < 1 {
				self.Weight = defaultWeight
			}
		}

		if self.ChannelFactory == nil {
			self.ChannelFactory = defaultServerChannelFactory{}
		}
	})

	return self
}

type ServerChannelFactory interface {
	CreateProduct(*ChannelPolicy, net.Conn, context.Context) (*ServerChannel, error)
	DestroyProduct(*ServerChannel)
}

const acceptTimeoutOfServer = 2 * time.Second
const defaultWeight = 5

type defaultServerChannelFactory struct{}

func (defaultServerChannelFactory) CreateProduct(productPolicy *ChannelPolicy, connection net.Conn, context_ context.Context) (*ServerChannel, error) {
	return (&ServerChannel{}).Initialize(productPolicy, connection, context_), nil
}

func (defaultServerChannelFactory) DestroyProduct(_ *ServerChannel) {
}
