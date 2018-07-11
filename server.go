package pbrpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type Server struct {
	serverAddress string
	channelPolicy *ChannelPolicy
	context1      context.Context
	context2      context.Context
	stop1         context.CancelFunc
	stop2         context.CancelFunc
	openness      int32
}

func (self *Server) Initialize(serverAddress string, channelPolicy *ChannelPolicy, context_ context.Context) *Server {
	if self.openness != 0 {
		panic(errors.New("pbrpc: server already initialized"))
	}

	if serverAddress == "" {
		serverAddress = defaultServerAddress
	}

	self.serverAddress = serverAddress
	self.channelPolicy = channelPolicy.Validate()

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

	var listener *net.TCPListener

	if listener2, e := net.Listen("tcp", self.serverAddress); e == nil {
		listener = listener2.(*net.TCPListener)
	} else {
		self.openness = -1
		return e
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
		go handleConnection(self.channelPolicy, self.context1, connection, &wg)
	}

	listener.Close()
	self.serverAddress = ""
	self.channelPolicy = nil
	self.context1 = nil
	self.context2 = nil
	self.stop1 = nil
	self.stop2 = nil
	self.openness = -1
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

const acceptTimeoutOfServer = 2 * time.Second

var defaultServerAddress = "127.0.0.1:8888"

func handleConnection(channelPolicy *ChannelPolicy, context_ context.Context, connection net.Conn, wg *sync.WaitGroup) {
	channel := (&ServerChannel{}).Initialize(channelPolicy, context_)
	e := channel.Run(connection)
	channelPolicy.Logger.Infof("connection handling: clientAddress=%#v, e=%#v", connection.RemoteAddr().String(), e.Error())
	wg.Done()
}
