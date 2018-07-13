package pbrpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"reflect"
	"unsafe"

	"github.com/let-z-go/toolkit/delay_pool"
)

type ClientChannel struct {
	channelBase
	serverAddresses delay_pool.DelayPool
}

func (self *ClientChannel) Initialize(policy *ChannelPolicy, serverAddresses []string, context_ context.Context) *ClientChannel {
	self.initialize(policy, true, context_)

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

func (self *ClientChannel) Run() error {
	if self.impl.isClosed() {
		return nil
	}

	var e error

	for {
		var value interface{}
		value, e = self.serverAddresses.GetValue(self.context)

		if e != nil {
			break
		}

		context_, cancel := context.WithDeadline(self.context, self.serverAddresses.WhenNextValueUsable())
		serverAddress := value.(string)
		e = self.impl.connect(context_, serverAddress)
		cancel()

		if e != nil {
			if e != io.EOF {
				if e == context.DeadlineExceeded {
					break
				}

				if _, ok := e.(net.Error); !ok {
					break
				}
			}

			continue
		}

		self.serverAddresses.Reset(nil, 9, self.impl.getTimeout())
		e = self.impl.dispatch(self.context)

		if e != nil {
			if e != io.EOF {
				if e == context.DeadlineExceeded {
					break
				}

				if _, ok := e.(net.Error); !ok {
					break
				}
			}

			continue
		}
	}

	self.impl.close()
	self.serverAddresses.Collect()
	return e
}

type ServerChannel struct {
	channelBase
}

func (self *ServerChannel) Initialize(policy *ChannelPolicy, context_ context.Context) *ServerChannel {
	self.initialize(policy, false, context_)
	return self
}

func (self *ServerChannel) Run(connection net.Conn) error {
	if self.impl.isClosed() {
		return nil
	}

	if e := self.impl.accept(self.context, connection); e != nil {
		self.impl.close()
		return e
	}

	e := self.impl.dispatch(self.context)
	self.impl.close()
	return e
}

type channelBase struct {
	impl     channelImpl
	context  context.Context
	stop     context.CancelFunc
	userData unsafe.Pointer
}

func (self *channelBase) CallMethod(
	context_ context.Context,
	serviceName string,
	methodIndex int32,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) (IncomingMessage, error) {
	var response IncomingMessage
	error_ := make(chan error, 1)

	callback := func(response2 IncomingMessage, errorCode ErrorCode) {
		if errorCode != 0 {
			error_ <- Error{false, errorCode, fmt.Sprintf("methodID=%v, request=%#v", representMethodID(serviceName, methodIndex), request)}
			return
		}

		response = response2
		error_ <- nil
	}

	if e := self.impl.callMethod(
		context_,
		serviceName,
		methodIndex,
		request,
		responseType,
		autoRetryMethodCall,
		callback,
	); e != nil {
		return nil, e
	}

	if context_ == nil {
		if e := <-error_; e != nil {
			return nil, e
		}
	} else {
		select {
		case e := <-error_:
			if e != nil {
				return nil, e
			}
		case <-context_.Done():
			return nil, context_.Err()
		}
	}

	return response, nil
}

func (self *channelBase) CallMethodWithoutReturn(
	context_ context.Context,
	serviceName string,
	methodIndex int32,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
) error {
	return self.impl.callMethod(
		context_,
		serviceName,
		methodIndex,
		request,
		responseType,
		autoRetryMethodCall,
		func(_ IncomingMessage, _ ErrorCode) {},
	)
}

func (self *channelBase) Stop() {
	if self.stop != nil {
		self.stop()
	}
}

func (self *channelBase) UserData() *unsafe.Pointer {
	return &self.userData
}

func (self *channelBase) initialize(policy *ChannelPolicy, isClientSide bool, context_ context.Context) *channelBase {
	self.impl.initialize(self, policy, isClientSide)

	if context_ == nil {
		context_ = context.Background()
	}

	self.context, self.stop = context.WithCancel(context_)
	return self
}
