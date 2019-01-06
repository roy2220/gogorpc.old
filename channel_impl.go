package pbrpc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/let-z-go/intrusive_containers/list"
	"github.com/let-z-go/toolkit/byte_stream"
	"github.com/let-z-go/toolkit/deque"
	"github.com/let-z-go/toolkit/logger"
	"github.com/let-z-go/toolkit/semaphore"
	"github.com/let-z-go/toolkit/uuid"

	"github.com/let-z-go/pbrpc/protocol"
)

const (
	ChannelNo ChannelState = iota
	ChannelNotConnected
	ChannelConnecting
	ChannelConnected
	ChannelNotAccepted
	ChannelAccepting
	ChannelAccepted
	ChannelClosed
)

type ChannelPolicy struct {
	Logger             logger.Logger
	Timeout            time.Duration
	IncomingWindowSize int32
	OutgoingWindowSize int32
	Transport          TransportPolicy

	serviceHandlers map[string]ServiceHandler
}

func (self *ChannelPolicy) registerServiceHandler(serviceHandler ServiceHandler) *ChannelPolicy {
	if self.serviceHandlers == nil {
		self.serviceHandlers = map[string]ServiceHandler{}
	}

	self.serviceHandlers[serviceHandler.X_GetName()] = serviceHandler
	return self
}

func (self *ChannelPolicy) validate() *ChannelPolicy {
	if self.Timeout == 0 {
		self.Timeout = defaultChannelTimeout
	} else {
		if self.Timeout < minChannelTimeout {
			self.Timeout = minChannelTimeout
		} else if self.Timeout > maxChannelTimeout {
			self.Timeout = maxChannelTimeout
		}
	}

	if self.IncomingWindowSize == 0 {
		self.IncomingWindowSize = defaultChannelWindowSize
	} else {
		if self.IncomingWindowSize < minChannelWindowSize {
			self.IncomingWindowSize = minChannelWindowSize
		} else if self.IncomingWindowSize > maxChannelWindowSize {
			self.IncomingWindowSize = maxChannelWindowSize
		}
	}

	if self.OutgoingWindowSize == 0 {
		self.OutgoingWindowSize = defaultChannelWindowSize
	} else {
		if self.OutgoingWindowSize < minChannelWindowSize {
			self.OutgoingWindowSize = minChannelWindowSize
		} else if self.OutgoingWindowSize > maxChannelWindowSize {
			self.OutgoingWindowSize = maxChannelWindowSize
		}
	}

	return self
}

type ChannelListener struct {
	stateChanges chan ChannelState
}

func (self *ChannelListener) StateChanges() <-chan ChannelState {
	return self.stateChanges
}

type ChannelState uint8

func (self ChannelState) GoString() string {
	switch self {
	case ChannelNo:
		return "<ChannelNo>"
	case ChannelNotConnected:
		return "<ChannelNotConnected>"
	case ChannelConnecting:
		return "<ChannelConnecting>"
	case ChannelConnected:
		return "<ChannelConnected>"
	case ChannelNotAccepted:
		return "<ChannelNotAccepted>"
	case ChannelAccepting:
		return "<ChannelAccepting>"
	case ChannelAccepted:
		return "<ChannelAccepted>"
	case ChannelClosed:
		return "<ChannelClosed>"
	default:
		return fmt.Sprintf("<ChannelState:%d>", self)
	}
}

var ChannelClosedError = errors.New("pbrpc: channel closed")
var IllFormedMessageError = errors.New("pbrpc: ill-formed message")

const defaultChannelTimeout = 6 * time.Second
const minChannelTimeout = 4 * time.Second
const maxChannelTimeout = 40 * time.Second
const defaultChannelWindowSize = 1 << 17
const minChannelWindowSize = 1
const maxChannelWindowSize = 1 << 20

type channelImpl struct {
	holder                   Channel
	policy                   *ChannelPolicy
	state                    int32
	lockOfListeners          sync.Mutex
	listeners                map[*ChannelListener]struct{}
	id                       uuid.UUID
	timeout                  time.Duration
	incomingWindowSize       int32
	outgoingWindowSize       int32
	nextSequenceNumber       int32
	transport                transport
	dequeOfMethodCalls       deque.Deque
	pendingMethodCalls       sync.Map
	pendingResultReturnCount int32
	wgOfPendingResultReturns sync.WaitGroup
	dequeOfResultReturns     *deque.Deque
}

func (self *channelImpl) initialize(holder Channel, policy *ChannelPolicy, isClientSide bool) *channelImpl {
	if self.state != 0 {
		panic(errors.New("pbrpc: channel already initialized"))
	}

	self.holder = holder
	self.policy = policy

	if isClientSide {
		self.state = int32(ChannelNotConnected)
	} else {
		self.state = int32(ChannelNotAccepted)
	}

	self.timeout = policy.Timeout
	self.incomingWindowSize = policy.IncomingWindowSize
	self.outgoingWindowSize = policy.OutgoingWindowSize
	self.dequeOfMethodCalls.Initialize(minChannelWindowSize)
	self.dequeOfResultReturns = (&deque.Deque{}).Initialize(math.MaxInt32)
	return self
}

func (self *channelImpl) close() {
	self.setState(ChannelClosed)
}

func (self *channelImpl) addListener(maxNumberOfStateChanges int) (*ChannelListener, error) {
	if self.isClosed() {
		return nil, ChannelClosedError
	}

	self.lockOfListeners.Lock()

	if self.isClosed() {
		self.lockOfListeners.Unlock()
		return nil, ChannelClosedError
	}

	listener := &ChannelListener{
		stateChanges: make(chan ChannelState, maxNumberOfStateChanges),
	}

	if self.listeners == nil {
		self.listeners = map[*ChannelListener]struct{}{}
	}

	self.listeners[listener] = struct{}{}
	self.lockOfListeners.Unlock()
	return listener, nil
}

func (self *channelImpl) removeListener(listener *ChannelListener) error {
	if self.isClosed() {
		return ChannelClosedError
	}

	close(listener.stateChanges)
	self.lockOfListeners.Lock()

	if self.isClosed() {
		self.lockOfListeners.Unlock()
		return ChannelClosedError
	}

	delete(self.listeners, listener)

	if len(self.listeners) == 0 {
		self.listeners = nil
	}

	self.lockOfListeners.Unlock()
	return nil
}

func (self *channelImpl) connect(connector Connector, context_ context.Context, serverAddress string, handshaker ClientHandshaker) error {
	self.policy.Logger.Infof("channel connection: id=%#v, serverAddress=%#v", self.id.String(), serverAddress)
	self.setState(ChannelConnecting)
	connection, e := connector.Connect(context_, serverAddress)

	if e != nil {
		return e
	}

	transport_ := (&transport{}).initialize(&self.policy.Transport, connection)

	greeting := protocol.Greeting{
		Channel: protocol.Greeting_Channel{
			Timeout:            int32(self.timeout / time.Millisecond),
			IncomingWindowSize: self.incomingWindowSize,
			OutgoingWindowSize: self.outgoingWindowSize,
		},
	}

	if !self.id.IsZero() {
		greeting.Channel.Id = self.id[:]
	}

	greeter := func(context_ context.Context, handshake []byte) ([]byte, error) {
		greeting.Handshake = handshake

		if e := transport_.write(func(byteStream *byte_stream.ByteStream) error {
			return byteStream.WriteDirectly(greeting.Size(), func(buffer []byte) error {
				_, e := greeting.MarshalTo(buffer)
				return e
			})
		}); e != nil {
			return nil, e
		}

		if e := transport_.flush(context_, minChannelTimeout); e != nil {
			return nil, e
		}

		data, e := transport_.peek(context_, minChannelTimeout)

		if e != nil {
			return nil, e
		}

		greeting.Reset()

		if e := greeting.Unmarshal(data); e != nil {
			return nil, e
		}

		if e := transport_.skip(data); e != nil {
			return nil, e
		}

		return greeting.Handshake, nil
	}

	if e := handshaker(self.holder.(*ClientChannel), context_, greeter); e != nil {
		transport_.close(true)
		return e
	}

	var outgoingWindowSize int32

	if self.id.IsZero() {
		outgoingWindowSize = minChannelWindowSize
	} else {
		outgoingWindowSize = self.outgoingWindowSize
	}

	copy(self.id[:], greeting.Channel.Id)
	self.timeout = time.Duration(greeting.Channel.Timeout) * time.Millisecond
	self.incomingWindowSize = greeting.Channel.IncomingWindowSize
	self.outgoingWindowSize = greeting.Channel.OutgoingWindowSize

	if e := self.dequeOfMethodCalls.CommitNodeRemovals(self.outgoingWindowSize - outgoingWindowSize); e != nil {
		transport_.close(true)
		return e
	}

	self.transport.close(true)
	self.transport = *transport_
	self.setState(ChannelConnected)
	self.policy.Logger.Infof("channel establishment: serverAddress=%#v, id=%#v, timeout=%#v, incomingWindowSize=%#v, outgoingWindowSize=%#v", serverAddress, self.id.String(), self.timeout, self.incomingWindowSize, self.outgoingWindowSize)
	return nil
}

func (self *channelImpl) accept(context_ context.Context, connection net.Conn, handshaker ServerHandshaker) error {
	clientAddress := connection.RemoteAddr().String()
	self.policy.Logger.Infof("channel acceptance: clientAddress=%#v", clientAddress)
	self.setState(ChannelAccepting)
	transport_ := (&transport{}).initialize(&self.policy.Transport, connection)
	data, e := transport_.peek(context_, minChannelTimeout)

	if e != nil {
		transport_.close(true)
		return e
	}

	var greeting protocol.Greeting

	if e := greeting.Unmarshal(data); e != nil {
		transport_.close(true)
		return e
	}

	if e := transport_.skip(data); e != nil {
		transport_.close(true)
		return e
	}

	var id uuid.UUID

	if len(greeting.Channel.Id) == 0 {
		id, e = uuid.GenerateUUID4()

		if e != nil {
			transport_.close(true)
			return e
		}

		greeting.Channel.Id = id[:]
	} else {
		copy(id[:], greeting.Channel.Id)
	}

	if timeout := time.Duration(greeting.Channel.Timeout) * time.Millisecond; timeout > self.policy.Timeout {
		if timeout > maxChannelTimeout {
			greeting.Channel.Timeout = int32(maxChannelTimeout / time.Millisecond)
		}
	} else {
		greeting.Channel.Timeout = int32(self.policy.Timeout / time.Millisecond)
	}

	if greeting.Channel.IncomingWindowSize < self.policy.OutgoingWindowSize {
		if greeting.Channel.IncomingWindowSize < minChannelWindowSize {
			greeting.Channel.IncomingWindowSize = minChannelWindowSize
		}
	} else {
		greeting.Channel.IncomingWindowSize = self.policy.OutgoingWindowSize
	}

	if greeting.Channel.OutgoingWindowSize < self.policy.IncomingWindowSize {
		if greeting.Channel.OutgoingWindowSize < minChannelWindowSize {
			greeting.Channel.OutgoingWindowSize = minChannelWindowSize
		}
	} else {
		greeting.Channel.OutgoingWindowSize = self.policy.IncomingWindowSize
	}

	greeting.Handshake, e = handshaker(self.holder.(*ServerChannel), context_, greeting.Handshake)

	if e != nil {
		transport_.close(true)
		return e
	}

	if e := transport_.write(func(byteStream *byte_stream.ByteStream) error {
		return byteStream.WriteDirectly(greeting.Size(), func(buffer []byte) error {
			_, e := greeting.MarshalTo(buffer)
			return e
		})
	}); e != nil {
		transport_.close(true)
		return e
	}

	if e := transport_.flush(context_, minChannelTimeout); e != nil {
		transport_.close(true)
		return e
	}

	self.id = id
	self.timeout = time.Duration(greeting.Channel.Timeout) * time.Millisecond
	self.incomingWindowSize = greeting.Channel.OutgoingWindowSize
	self.outgoingWindowSize = greeting.Channel.IncomingWindowSize

	if e := self.dequeOfMethodCalls.CommitNodeRemovals(self.outgoingWindowSize - minChannelWindowSize); e != nil {
		transport_.close(true)
		return e
	}

	self.transport.close(true)
	self.transport = *transport_
	self.setState(ChannelAccepted)
	self.policy.Logger.Infof("channel establishment: clientAddress=%#v, id=%#v, timeout=%#v, incomingWindowSize=%#v, outgoingWindowSize=%#v", clientAddress, self.id.String(), self.timeout, self.incomingWindowSize, self.outgoingWindowSize)
	return nil
}

func (self *channelImpl) dispatch(context_ context.Context) error {
	if state := self.getState(); state != ChannelConnected && state != ChannelAccepted {
		panic(invalidChannelStateError{fmt.Sprintf("state=%#v", state)})
	}

	context2, cancel := context.WithCancel(context_)
	errors_ := make(chan error, 2)

	go func() {
		errors_ <- self.sendMessages(context2)
	}()

	go func() {
		errors_ <- self.receiveMessages(context2)
	}()

	e := <-errors_
	cancel()
	<-errors_
	return e
}

func (self *channelImpl) callMethod(
	context_ context.Context,
	serviceName string,
	methodName string,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
	callback func(interface{}, ErrorCode),
) error {
	if self.isClosed() {
		return Error{true, self.getErrorCode(), fmt.Sprintf("methodID=%v, request=%#v", representMethodID(serviceName, methodName), request)}
	}

	methodCall_ := poolOfMethodCalls.Get().(*methodCall)

	if contextVars_, ok := GetContextVars(context_); ok {
		methodCall_.traceID = contextVars_.TraceID
		methodCall_.spanParentID = contextVars_.SpanID
		methodCall_.spanID = contextVars_.NextSpanID
		methodCall_.nextSpanID = &contextVars_.NextSpanID
	} else {
		traceID, e := uuid.GenerateUUID4()

		if e != nil {
			poolOfMethodCalls.Put(methodCall_)
			return e
		}

		methodCall_.traceID = traceID
		methodCall_.spanParentID = 0
		methodCall_.spanID = 1
		methodCall_.nextSpanID = &methodCall_.spanID // dummy buffer
	}

	methodCall_.serviceName = serviceName
	methodCall_.methodName = methodName
	methodCall_.request = request
	methodCall_.responseType = responseType
	methodCall_.autoRetry = autoRetryMethodCall
	methodCall_.callback = callback

	if e := self.dequeOfMethodCalls.AppendNode(context_, &methodCall_.listNode); e != nil {
		if e == semaphore.SemaphoreClosedError {
			e = Error{true, self.getErrorCode(), fmt.Sprintf("methodID=%v, request=%#v", representMethodID(serviceName, methodName), request)}
		}

		return e
	}

	return nil
}

func (self *channelImpl) isClosed() bool {
	return self.getErrorCode() != 0
}

func (self *channelImpl) getTimeout() time.Duration {
	return self.timeout
}

func (self *channelImpl) setState(newState ChannelState) {
	oldState := self.getState()
	errorCode := ErrorCode(0)

	switch oldState {
	case ChannelNotConnected:
		switch newState {
		case ChannelConnecting:
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case ChannelConnecting:
		switch newState {
		case ChannelConnecting:
			return
		case ChannelConnected:
		case ChannelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case ChannelConnected:
		switch newState {
		case ChannelConnecting:
			errorCode = ErrorChannelBroken
		case ChannelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case ChannelNotAccepted:
		switch newState {
		case ChannelAccepting:
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case ChannelAccepting:
		switch newState {
		case ChannelAccepted:
		case ChannelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case ChannelAccepted:
		switch newState {
		case ChannelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	default:
		panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
	}

	atomic.StoreInt32(&self.state, int32(newState))
	self.policy.Logger.Infof("channel state change: id=%#v, oldState=%#v, newState=%#v", self.id.String(), oldState, newState)
	self.lockOfListeners.Lock()

	for listener := range self.listeners {
		listener.stateChanges <- newState
	}

	self.lockOfListeners.Unlock()

	if errorCode != 0 {
		methodCallsAreRetriable := errorCode == ErrorChannelBroken

		if errorCode2 := self.getErrorCode(); errorCode2 == 0 {
			list_ := (&list.List{}).Initialize()
			retriedMethodCallCount := int32(0)
			completedMethodCallCount := int32(0)

			self.pendingMethodCalls.Range(func(key interface{}, value interface{}) bool {
				self.pendingMethodCalls.Delete(key)
				methodCall_ := value.(*methodCall)

				if methodCallsAreRetriable && methodCall_.autoRetry {
					list_.AppendNode(&methodCall_.listNode)
					retriedMethodCallCount++
				} else {
					methodCall_.callback(nil, errorCode)
					completedMethodCallCount++
					poolOfMethodCalls.Put(methodCall_)
				}

				return true
			})

			self.dequeOfMethodCalls.DiscardNodeRemovals(list_, retriedMethodCallCount)
			self.dequeOfMethodCalls.CommitNodeRemovals(completedMethodCallCount)
			atomic.StoreInt32(&self.pendingResultReturnCount, 0)
			list_.Initialize()
			self.dequeOfResultReturns.Close(list_)
			self.dequeOfResultReturns = (&deque.Deque{}).Initialize(math.MaxInt32)
			getListNode := list_.GetNodesSafely()

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				resultReturn_ := (*resultReturn)(listNode.GetContainer(unsafe.Offsetof(resultReturn{}.listNode)))
				poolOfResultReturns.Put(resultReturn_)
			}
		} else {
			self.holder = nil
			self.policy = nil

			{
				self.lockOfListeners.Lock()
				listeners := self.listeners
				self.listeners = nil
				self.lockOfListeners.Unlock()

				for listener := range listeners {
					close(listener.stateChanges)
				}
			}

			self.transport.close(false)

			{
				list_ := (&list.List{}).Initialize()
				self.dequeOfMethodCalls.Close(list_)
				getListNode := list_.GetNodesSafely()

				for listNode := getListNode(); listNode != nil; listNode = getListNode() {
					methodCall_ := (*methodCall)(listNode.GetContainer(unsafe.Offsetof(methodCall{}.listNode)))
					methodCall_.callback(nil, errorCode2)
					poolOfMethodCalls.Put(methodCall_)
				}
			}

			self.pendingMethodCalls.Range(func(key interface{}, value interface{}) bool {
				self.pendingMethodCalls.Delete(key)
				methodCall_ := value.(*methodCall)

				if methodCallsAreRetriable && methodCall_.autoRetry {
					methodCall_.callback(nil, errorCode2)
				} else {
					methodCall_.callback(nil, errorCode)
				}

				poolOfMethodCalls.Put(methodCall_)
				return true
			})

			{
				list_ := (&list.List{}).Initialize()
				self.dequeOfResultReturns.Close(list_)
				self.dequeOfResultReturns = nil
				getListNode := list_.GetNodesSafely()

				for listNode := getListNode(); listNode != nil; listNode = getListNode() {
					resultReturn_ := (*resultReturn)(listNode.GetContainer(unsafe.Offsetof(resultReturn{}.listNode)))
					poolOfResultReturns.Put(resultReturn_)
				}
			}

			self.wgOfPendingResultReturns.Wait()
		}
	}
}

func (self *channelImpl) sendMessages(context_ context.Context) error {
	context2, cancel := context.WithCancel(context_)
	errors_ := make(chan error, 2)

	type task struct {
		list              *list.List
		numberOfListNodes int32
	}

	tasksOfMethodCalls := make(chan task)
	tasksOfResultReturns := make(chan task)

	go func() {
		list1 := (&list.List{}).Initialize()
		list2 := (&list.List{}).Initialize()

		for {
			numberOfListNodes, e := self.dequeOfMethodCalls.RemoveAllNodes(context2, false, list1)

			if e != nil {
				errors_ <- e
				return
			}

			select {
			case tasksOfMethodCalls <- task{list1, numberOfListNodes}:
			case <-context2.Done():
				self.dequeOfMethodCalls.DiscardNodeRemovals(list1, numberOfListNodes)
				errors_ <- context2.Err()
				return
			}

			list1, list2 = list2, list1
			list1.Initialize()
		}
	}()

	go func() {
		list1 := (&list.List{}).Initialize()
		list2 := (&list.List{}).Initialize()

		for {
			numberOfListNodes, e := self.dequeOfResultReturns.RemoveAllNodes(context2, true, list1)

			if e != nil {
				errors_ <- e
				return
			}

			select {
			case tasksOfResultReturns <- task{list1, numberOfListNodes}:
			case <-context2.Done():
				errors_ <- context2.Err()
				return
			}

			list1, list2 = list2, list1
			list1.Initialize()
		}
	}()

	var taskOfMethodCalls task
	var taskOfResultReturns task

	cleanup := func(ok bool) {
		if taskOfMethodCalls.list != nil {
			if ok {
				getListNode := taskOfMethodCalls.list.GetNodes()

				for listNode := getListNode(); listNode != nil; listNode = getListNode() {
					methodCall_ := (*methodCall)(listNode.GetContainer(unsafe.Offsetof(methodCall{}.listNode)))
					self.pendingMethodCalls.Store(methodCall_.sequenceNumber, methodCall_)
				}
			} else {
				self.dequeOfMethodCalls.DiscardNodeRemovals(taskOfMethodCalls.list, taskOfMethodCalls.numberOfListNodes)
			}
		}

		if taskOfResultReturns.list != nil {
			getListNode := taskOfResultReturns.list.GetNodesSafely()

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				resultReturn_ := (*resultReturn)(listNode.GetContainer(unsafe.Offsetof(resultReturn{}.listNode)))
				poolOfResultReturns.Put(resultReturn_)
			}

			if ok {
				atomic.AddInt32(&self.pendingResultReturnCount, -taskOfResultReturns.numberOfListNodes)
			}
		}

		if !ok {
			cancel()
			<-errors_
			<-errors_
		}
	}

	for {
		taskOfMethodCalls = task{nil, 0}
		taskOfResultReturns = task{nil, 0}

		select {
		case e := <-errors_:
			cancel()
			<-errors_
			return e
		case taskOfMethodCalls = <-tasksOfMethodCalls:
			select {
			case taskOfResultReturns = <-tasksOfResultReturns:
			default:
			}
		case taskOfResultReturns = <-tasksOfResultReturns:
			select {
			case taskOfMethodCalls = <-tasksOfMethodCalls:
			default:
			}
		case <-time.After(self.getMinHeartbeatInterval()):
			heartbeat := protocol.Heartbeat{}
			heartbeatSize := heartbeat.Size()

			if e := self.transport.write(func(byteStream *byte_stream.ByteStream) error {
				return byteStream.WriteDirectly(3+heartbeatSize, func(buffer []byte) error {
					buffer[0] = uint8(protocol.MESSAGE_HEARTBEAT)
					buffer[1] = uint8(heartbeatSize >> 8)
					buffer[2] = uint8(heartbeatSize)
					_, e := heartbeat.MarshalTo(buffer[3:])
					return e
				})
			}); e != nil {
				cancel()
				<-errors_
				<-errors_
				return e
			}
		}

		if taskOfMethodCalls.list != nil {
			getListNode := taskOfMethodCalls.list.GetNodesSafely()
			completedMethodCallCount := int32(0)

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				methodCall_ := (*methodCall)(listNode.GetContainer(unsafe.Offsetof(methodCall{}.listNode)))
				methodCall_.sequenceNumber = self.getSequenceNumber()

				requestHeader := protocol.RequestHeader{
					TraceId:        methodCall_.traceID[:],
					SpanParentId:   methodCall_.spanParentID,
					SpanId:         methodCall_.spanID,
					SequenceNumber: methodCall_.sequenceNumber,
					ServiceName:    methodCall_.serviceName,
					MethodName:     methodCall_.methodName,
				}

				requestHeaderSize := requestHeader.Size()

				if e := self.transport.write(func(byteStream *byte_stream.ByteStream) error {
					return byteStream.WriteDirectly(3+requestHeaderSize+methodCall_.request.Size(), func(buffer []byte) error {
						buffer[0] = uint8(protocol.MESSAGE_REQUEST)
						buffer[1] = uint8(requestHeaderSize >> 8)
						buffer[2] = uint8(requestHeaderSize)

						if _, e := requestHeader.MarshalTo(buffer[3:]); e != nil {
							return e
						}

						if _, e := methodCall_.request.MarshalTo(buffer[3+requestHeaderSize:]); e != nil {
							return e
						}

						return nil
					})
				}); e != nil {
					if e == PacketPayloadTooLargeError {
						listNode.Remove()
						taskOfMethodCalls.numberOfListNodes--
						methodCall_.callback(nil, ErrorPacketPayloadTooLarge)
						completedMethodCallCount++
						poolOfMethodCalls.Put(methodCall_)
						continue
					}

					cleanup(false)
					return e
				}
			}

			self.dequeOfMethodCalls.CommitNodeRemovals(completedMethodCallCount)
		}

		if taskOfResultReturns.list != nil {
			getListNode := taskOfResultReturns.list.GetNodes()

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				resultReturn_ := (*resultReturn)(listNode.GetContainer(unsafe.Offsetof(resultReturn{}.listNode)))

				responseHeader := protocol.ResponseHeader{
					NextSpanId:     resultReturn_.nextSpanID,
					SequenceNumber: resultReturn_.sequenceNumber,
					ErrorCode:      int32(resultReturn_.errorCode),
				}

				responseHeaderSize := responseHeader.Size()

				if e := self.transport.write(func(byteStream *byte_stream.ByteStream) error {
					if resultReturn_.errorCode == 0 {
						return byteStream.WriteDirectly(3+responseHeaderSize+resultReturn_.response.Size(), func(buffer []byte) error {
							buffer[0] = uint8(protocol.MESSAGE_RESPONSE)
							buffer[1] = uint8(responseHeaderSize >> 8)
							buffer[2] = uint8(responseHeaderSize)

							if _, e := responseHeader.MarshalTo(buffer[3:]); e != nil {
								return e
							}

							if _, e := resultReturn_.response.MarshalTo(buffer[3+responseHeaderSize:]); e != nil {
								return e
							}

							return nil
						})
					} else {
						return byteStream.WriteDirectly(3+responseHeaderSize, func(buffer []byte) error {
							buffer[0] = uint8(protocol.MESSAGE_RESPONSE)
							buffer[1] = uint8(responseHeaderSize >> 8)
							buffer[2] = uint8(responseHeaderSize)
							_, e := responseHeader.MarshalTo(buffer[3:])
							return e
						})
					}
				}); e != nil {
					cleanup(false)
					return e
				}
			}
		}

		cleanup(true)

		for {
			if e := self.transport.flush(context_, self.getMinHeartbeatInterval()); e != nil {
				if e2, ok := e.(*net.OpError); ok && e2.Timeout() {
					continue
				}

				cancel()
				<-errors_
				<-errors_
				return e
			}

			break
		}
	}
}

func (self *channelImpl) receiveMessages(context_ context.Context) error {
	for {
		timeoutCount := 0
		var data [][]byte

		for {
			var e error
			data, e = self.transport.peekInBatch(context_, self.getMinHeartbeatInterval())

			if e != nil {
				if e2, ok := e.(*net.OpError); ok && e2.Timeout() {
					timeoutCount++

					if timeoutCount == 2 {
						return e
					}

					continue
				}

				return e
			}

			break
		}

		oldPendingResultReturnCount := atomic.LoadInt32(&self.pendingResultReturnCount)
		newPendingResultReturnCount := oldPendingResultReturnCount
		completedMethodCallCount := int32(0)

		for _, data2 := range data {
			if len(data2) < 3 {
				return IllFormedMessageError
			}

			messageType := protocol.MessageType(data2[0])
			messageHeaderSize := (int(data2[1]) << 8) | int(data2[2])

			if len(data2) < 3+messageHeaderSize {
				return IllFormedMessageError
			}

			messageHeader := data2[3 : 3+messageHeaderSize]
			messagePayload := data2[3+messageHeaderSize:]

			switch messageType {
			case protocol.MESSAGE_REQUEST:
				var requestHeader protocol.RequestHeader

				if e := requestHeader.Unmarshal(messageHeader); e != nil {
					return IllFormedMessageError
				}

				nextSpanID := requestHeader.SpanId + 1
				newPendingResultReturnCount++

				if newPendingResultReturnCount > self.incomingWindowSize {
					returnResult(self.dequeOfResultReturns, nextSpanID, requestHeader.SequenceNumber, ErrorTooManyRequests, nil)
					continue
				}

				serviceHandler, ok := self.policy.serviceHandlers[requestHeader.ServiceName]

				if !ok {
					returnResult(self.dequeOfResultReturns, nextSpanID, requestHeader.SequenceNumber, ErrorNotFound, nil)
					continue
				}

				methodTable := serviceHandler.X_GetMethodTable()
				methodRecord, ok := methodTable.Search(requestHeader.MethodName)

				if !ok {
					returnResult(self.dequeOfResultReturns, nextSpanID, requestHeader.SequenceNumber, ErrorNotFound, nil)
					continue
				}

				request := reflect.New(methodRecord.RequestType).Interface().(IncomingMessage)

				if e := request.Unmarshal(messagePayload); e != nil {
					returnResult(self.dequeOfResultReturns, nextSpanID, requestHeader.SequenceNumber, ErrorBadRequest, nil)
					continue
				}

				self.wgOfPendingResultReturns.Add(1)

				go func(
					methodHandlingInfo MethodHandlingInfo,
					dequeOfResultReturns *deque.Deque,
					sequenceNumber int32,
				) {
					methodHandlingInfo.Context = bindContextVars(methodHandlingInfo.Context, &methodHandlingInfo.ContextVars)
					response, errorCode := methodHandlingInfo.ServiceHandler.X_HandleMethod(&methodHandlingInfo)
					returnResult(dequeOfResultReturns, methodHandlingInfo.ContextVars.NextSpanID, sequenceNumber, errorCode, response)
					self.wgOfPendingResultReturns.Done()
				}(
					MethodHandlingInfo{
						ServiceHandler: serviceHandler,
						MethodRecord:   methodRecord,
						Context:        context_,

						ContextVars: ContextVars{
							Channel:      self.holder,
							TraceID:      uuid.UUIDFromBytes(requestHeader.TraceId),
							SpanParentID: requestHeader.SpanParentId,
							SpanID:       requestHeader.SpanId,
							NextSpanID:   nextSpanID,
						},

						Request: request,

						logger: &self.policy.Logger,
					},

					self.dequeOfResultReturns,
					requestHeader.SequenceNumber,
				)
			case protocol.MESSAGE_RESPONSE:
				var responseHeader protocol.ResponseHeader

				if e := responseHeader.Unmarshal(messageHeader); e != nil {
					return IllFormedMessageError
				}

				if value, ok := self.pendingMethodCalls.Load(responseHeader.SequenceNumber); ok {
					self.pendingMethodCalls.Delete(responseHeader.SequenceNumber)
					methodCall_ := value.(*methodCall)
					*methodCall_.nextSpanID = responseHeader.NextSpanId

					if responseHeader.ErrorCode == 0 {
						response := reflect.New(methodCall_.responseType).Interface().(IncomingMessage)

						if e := response.Unmarshal(messagePayload); e != nil {
							return IllFormedMessageError
						}

						methodCall_.callback(response, 0)
					} else {
						methodCall_.callback(nil, ErrorCode(responseHeader.ErrorCode))
					}

					completedMethodCallCount++
					poolOfMethodCalls.Put(methodCall_)
				} else {
					self.policy.Logger.Warningf("ignored response: id=%#v, responseHeader=%#v", self.id.String(), responseHeader)
				}
			case protocol.MESSAGE_HEARTBEAT:
				var heartbeat protocol.Heartbeat

				if e := heartbeat.Unmarshal(messageHeader); e != nil {
					return IllFormedMessageError
				}
			default:
				return IllFormedMessageError
			}
		}

		atomic.AddInt32(&self.pendingResultReturnCount, newPendingResultReturnCount-oldPendingResultReturnCount)
		self.dequeOfMethodCalls.CommitNodeRemovals(completedMethodCallCount)

		if e := self.transport.skipInBatch(data); e != nil {
			return e
		}
	}
}

func (self *channelImpl) getErrorCode() ErrorCode {
	return channelState2ErrorCode[self.getState()]
}

func (self *channelImpl) getState() ChannelState {
	return ChannelState(atomic.LoadInt32(&self.state))
}

func (self *channelImpl) getMinHeartbeatInterval() time.Duration {
	return self.timeout / 3
}

func (self *channelImpl) getSequenceNumber() int32 {
	sequenceNumber := self.nextSequenceNumber
	self.nextSequenceNumber = int32(uint32(sequenceNumber+1) & 0xFFFFFFF)
	return sequenceNumber
}

type methodCall struct {
	listNode       list.ListNode
	traceID        uuid.UUID
	spanParentID   int32
	spanID         int32
	nextSpanID     *int32
	sequenceNumber int32
	serviceName    string
	methodName     string
	request        OutgoingMessage
	responseType   reflect.Type
	autoRetry      bool
	callback       func(interface{}, ErrorCode)
}

type resultReturn struct {
	listNode       list.ListNode
	nextSpanID     int32
	sequenceNumber int32
	errorCode      ErrorCode
	response       OutgoingMessage
}

type invalidChannelStateError struct {
	context string
}

func (self invalidChannelStateError) Error() string {
	result := "pbrpc: invalid channel state"

	if self.context != "" {
		result += ": " + self.context
	}

	return result
}

var channelState2ErrorCode = [...]ErrorCode{
	ChannelNo:           ErrorChannelTimedOut,
	ChannelNotConnected: 0,
	ChannelConnecting:   0,
	ChannelConnected:    0,
	ChannelNotAccepted:  0,
	ChannelAccepting:    0,
	ChannelAccepted:     0,
	ChannelClosed:       ErrorChannelTimedOut,
}

var poolOfMethodCalls = sync.Pool{New: func() interface{} { return &methodCall{} }}
var poolOfResultReturns = sync.Pool{New: func() interface{} { return &resultReturn{} }}

func returnResult(dequeOfResultReturns *deque.Deque, nextSpanID int32, sequenceNumber int32, errorCode ErrorCode, response OutgoingMessage) error {
	resultReturn_ := poolOfResultReturns.Get().(*resultReturn)
	resultReturn_.nextSpanID = nextSpanID
	resultReturn_.sequenceNumber = sequenceNumber
	resultReturn_.errorCode = errorCode
	resultReturn_.response = response
	return dequeOfResultReturns.AppendNode(nil, &resultReturn_.listNode)
}
