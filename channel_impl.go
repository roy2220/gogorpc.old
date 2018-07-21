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

type ChannelPolicy struct {
	Logger             logger.Logger
	Timeout            time.Duration
	IncomingWindowSize int32
	OutgoingWindowSize int32
	Transport          TransportPolicy
	serviceHandlers    map[string]ServiceHandler
	validateOnce       sync.Once
}

func (self *ChannelPolicy) RegisterServiceHandler(serviceHandler ServiceHandler) *ChannelPolicy {
	if self.serviceHandlers == nil {
		self.serviceHandlers = map[string]ServiceHandler{}
	}

	self.serviceHandlers[serviceHandler.X_GetName()] = serviceHandler
	return self
}

func (self *ChannelPolicy) Validate() *ChannelPolicy {
	self.validateOnce.Do(func() {
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
	})

	return self
}

var IllFormedMessageError = errors.New("pbrpc: ill-formed message")

const (
	channelNo channelState = iota
	channelNotConnected
	channelConnecting
	channelConnected
	channelNotAccepted
	channelAccepting
	channelAccepted
	channelClosed
)

const defaultChannelTimeout = 6 * time.Second
const minChannelTimeout = 4 * time.Second
const maxChannelTimeout = 40 * time.Second
const defaultChannelWindowSize = 1 << 10
const minChannelWindowSize = 1 << 4
const maxChannelWindowSize = 1 << 16

type channelImpl struct {
	holder                   Channel
	policy                   *ChannelPolicy
	state                    int32
	id                       *uuid.UUID
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
	self.policy = policy.Validate()

	if isClientSide {
		self.state = int32(channelNotConnected)
	} else {
		self.state = int32(channelNotAccepted)
	}

	self.timeout = policy.Timeout
	self.incomingWindowSize = policy.IncomingWindowSize
	self.outgoingWindowSize = policy.OutgoingWindowSize
	self.dequeOfMethodCalls.Initialize(minChannelWindowSize)
	self.dequeOfResultReturns = (&deque.Deque{}).Initialize(math.MaxInt32)
	return self
}

func (self *channelImpl) close() {
	self.setState(channelClosed)
}

func (self *channelImpl) connect(context_ context.Context, serverAddress string) error {
	self.policy.Logger.Infof("channel connection: id=%#v, serverAddress=%#v", channelIDToString(self.id), serverAddress)
	self.setState(channelConnecting)
	var transport_ transport

	if e := transport_.connect(context_, &self.policy.Transport, serverAddress); e != nil {
		return e
	}

	handshake := protocol.Handshake{
		Timeout:            int32(self.policy.Timeout / time.Millisecond),
		IncomingWindowSize: self.policy.IncomingWindowSize,
		OutgoingWindowSize: self.policy.OutgoingWindowSize,
	}

	if self.id != nil {
		handshake.Id = (*self.id)[:]
	}

	if e := transport_.write(func(byteStream *byte_stream.ByteStream) error {
		return byteStream.WriteDirectly(handshake.Size(), func(buffer []byte) error {
			_, e := handshake.MarshalTo(buffer)
			return e
		})
	}); e != nil {
		transport_.close()
		return e
	}

	if e := transport_.flush(context_, minChannelTimeout); e != nil {
		transport_.close()
		return e
	}

	data, e := transport_.peek(context_, minChannelTimeout)

	if e != nil {
		transport_.close()
		return e
	}

	handshake.Reset()

	if e := handshake.Unmarshal(data); e != nil {
		transport_.close()
		return e
	}

	if e := transport_.skip(data); e != nil {
		transport_.close()
		return e
	}

	var id uuid.UUID
	copy(id[:], handshake.Id)
	self.id = &id
	self.timeout = time.Duration(handshake.Timeout) * time.Millisecond
	self.incomingWindowSize = handshake.IncomingWindowSize
	self.outgoingWindowSize = handshake.OutgoingWindowSize

	if e := self.dequeOfMethodCalls.CommitNodeRemovals(self.outgoingWindowSize - minChannelWindowSize); e != nil {
		transport_.close()
		return e
	}

	self.transport.close()
	self.transport = transport_
	self.setState(channelConnected)
	self.policy.Logger.Infof("channel establishment: serverAddress=%#v, id=%#v, timeout=%#v, incomingWindowSize=%#v, outgoingWindowSize=%#v", serverAddress, channelIDToString(&id), self.timeout, self.incomingWindowSize, self.outgoingWindowSize)
	return nil
}

func (self *channelImpl) accept(context_ context.Context, connection net.Conn) error {
	clientAddress := connection.RemoteAddr().String()
	self.policy.Logger.Infof("channel acceptance: clientAddress=%#v", clientAddress)
	self.setState(channelAccepting)
	var transport_ transport
	transport_.accept(&self.policy.Transport, connection)
	data, e := transport_.peek(context_, minChannelTimeout)

	if e != nil {
		transport_.close()
		return e
	}

	var handshake protocol.Handshake

	if e := handshake.Unmarshal(data); e != nil {
		transport_.close()
		return e
	}

	if e := transport_.skip(data); e != nil {
		transport_.close()
		return e
	}

	var id uuid.UUID

	if len(handshake.Id) == 0 {
		id, e = uuid.GenerateUUID4()

		if e != nil {
			transport_.close()
			return e
		}

		handshake.Id = id[:]
	} else {
		copy(id[:], handshake.Id)
	}

	if timeout := time.Duration(handshake.Timeout) * time.Millisecond; timeout > self.policy.Timeout {
		if timeout > maxChannelTimeout {
			handshake.Timeout = int32(maxChannelTimeout / time.Millisecond)
		}
	} else {
		handshake.Timeout = int32(self.policy.Timeout / time.Millisecond)
	}

	if handshake.IncomingWindowSize < self.policy.OutgoingWindowSize {
		if handshake.IncomingWindowSize < minChannelWindowSize {
			handshake.IncomingWindowSize = minChannelWindowSize
		}
	} else {
		handshake.IncomingWindowSize = self.policy.OutgoingWindowSize
	}

	if handshake.OutgoingWindowSize < self.policy.IncomingWindowSize {
		if handshake.OutgoingWindowSize < minChannelWindowSize {
			handshake.OutgoingWindowSize = minChannelWindowSize
		}
	} else {
		handshake.OutgoingWindowSize = self.policy.IncomingWindowSize
	}

	if e := transport_.write(func(byteStream *byte_stream.ByteStream) error {
		return byteStream.WriteDirectly(handshake.Size(), func(buffer []byte) error {
			_, e := handshake.MarshalTo(buffer)
			return e
		})
	}); e != nil {
		transport_.close()
		return e
	}

	if e := transport_.flush(context_, minChannelTimeout); e != nil {
		transport_.close()
		return e
	}

	self.id = &id
	self.timeout = time.Duration(handshake.Timeout) * time.Millisecond
	self.incomingWindowSize = handshake.OutgoingWindowSize
	self.outgoingWindowSize = handshake.IncomingWindowSize

	if e := self.dequeOfMethodCalls.CommitNodeRemovals(self.outgoingWindowSize - minChannelWindowSize); e != nil {
		transport_.close()
		return e
	}

	self.transport.close()
	self.transport = transport_
	self.setState(channelAccepted)
	self.policy.Logger.Infof("channel establishment: clientAddress=%#v, id=%#v, timeout=%#v, incomingWindowSize=%#v, outgoingWindowSize=%#v", clientAddress, channelIDToString(&id), self.timeout, self.incomingWindowSize, self.outgoingWindowSize)
	return nil
}

func (self *channelImpl) dispatch(context_ context.Context) error {
	if state := self.getState(); state != channelConnected && state != channelAccepted {
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
	callback func(IncomingMessage, ErrorCode),
) error {
	if self.isClosed() {
		return Error{true, self.getErrorCode(), fmt.Sprintf("methodID=%v, request=%#v", representMethodID(serviceName, methodName), request)}
	}

	methodCall_ := poolOfMethodCalls.Get().(*methodCall)
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

func (self *channelImpl) setState(newState channelState) {
	oldState := self.getState()
	errorCode := ErrorCode(0)

	switch oldState {
	case channelNotConnected:
		switch newState {
		case channelConnecting:
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case channelConnecting:
		switch newState {
		case channelConnecting:
			return
		case channelConnected:
		case channelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case channelConnected:
		switch newState {
		case channelConnecting:
			errorCode = ErrorChannelBroken
		case channelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case channelNotAccepted:
		switch newState {
		case channelAccepting:
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case channelAccepting:
		switch newState {
		case channelAccepted:
		case channelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case channelAccepted:
		switch newState {
		case channelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	default:
		panic(invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
	}

	atomic.StoreInt32(&self.state, int32(newState))
	self.policy.Logger.Infof("channel state change: id=%#v, oldState=%#v, newState=%#v", channelIDToString(self.id), oldState, newState)

	if errorCode != 0 {
		methodCallsAreRetriable := errorCode == ErrorChannelBroken

		if errorCode2 := self.getErrorCode(); errorCode2 == 0 {
			list_ := (&list.List{}).Initialize()
			retriedMethodCallCount := int32(0)

			self.pendingMethodCalls.Range(func(key interface{}, value interface{}) bool {
				self.pendingMethodCalls.Delete(key)
				methodCall_ := value.(*methodCall)

				if methodCallsAreRetriable && methodCall_.autoRetry {
					list_.AppendNode(&methodCall_.listNode)
					retriedMethodCallCount++
				} else {
					methodCall_.callback(nil, errorCode)
					poolOfMethodCalls.Put(methodCall_)
				}

				return true
			})

			self.dequeOfMethodCalls.DiscardNodeRemovals(list_, retriedMethodCallCount)
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
			self.id = nil
			self.transport.close()

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
				return byteStream.WriteDirectly(2+heartbeatSize, func(buffer []byte) error {
					buffer[0] = uint8(protocol.MESSAGE_HEARTBEAT)
					buffer[1] = uint8(heartbeatSize)
					_, e := heartbeat.MarshalTo(buffer[2:])
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

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				methodCall_ := (*methodCall)(listNode.GetContainer(unsafe.Offsetof(methodCall{}.listNode)))
				methodCall_.sequenceNumber = self.getSequenceNumber()

				requestHeader := protocol.RequestHeader{
					SequenceNumber: methodCall_.sequenceNumber,
					ServiceName:    methodCall_.serviceName,
					MethodName:     methodCall_.methodName,
				}

				requestHeaderSize := requestHeader.Size()

				if e := self.transport.write(func(byteStream *byte_stream.ByteStream) error {
					return byteStream.WriteDirectly(2+requestHeaderSize+methodCall_.request.Size(), func(buffer []byte) error {
						buffer[0] = uint8(protocol.MESSAGE_REQUEST)
						buffer[1] = uint8(requestHeaderSize)

						if _, e := requestHeader.MarshalTo(buffer[2:]); e != nil {
							return e
						}

						if _, e := methodCall_.request.MarshalTo(buffer[2+requestHeaderSize:]); e != nil {
							return e
						}

						return nil
					})
				}); e != nil {
					if e == PacketPayloadTooLargeError {
						methodCall_.callback(nil, ErrorPacketPayloadTooLarge)
						listNode.Remove()
						taskOfMethodCalls.numberOfListNodes--
						poolOfMethodCalls.Put(methodCall_)
						continue
					}

					cleanup(false)
					return e
				}
			}
		}

		if taskOfResultReturns.list != nil {
			getListNode := taskOfResultReturns.list.GetNodes()

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				resultReturn_ := (*resultReturn)(listNode.GetContainer(unsafe.Offsetof(resultReturn{}.listNode)))

				responseHeader := protocol.ResponseHeader{
					SequenceNumber: resultReturn_.sequenceNumber,
					ErrorCode:      int32(resultReturn_.errorCode),
				}

				responseHeaderSize := responseHeader.Size()

				if e := self.transport.write(func(byteStream *byte_stream.ByteStream) error {
					if resultReturn_.errorCode == 0 {
						return byteStream.WriteDirectly(2+responseHeaderSize+resultReturn_.response.Size(), func(buffer []byte) error {
							buffer[0] = uint8(protocol.MESSAGE_RESPONSE)
							buffer[1] = uint8(responseHeaderSize)

							if _, e := responseHeader.MarshalTo(buffer[2:]); e != nil {
								return e
							}

							if _, e := resultReturn_.response.MarshalTo(buffer[2+responseHeaderSize:]); e != nil {
								return e
							}

							return nil
						})
					} else {
						return byteStream.WriteDirectly(2+responseHeaderSize, func(buffer []byte) error {
							buffer[0] = uint8(protocol.MESSAGE_RESPONSE)
							buffer[1] = uint8(responseHeaderSize)
							_, e := responseHeader.MarshalTo(buffer[2:])
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
				if e, ok := e.(*net.OpError); ok && e.Timeout() {
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
				if e, ok := e.(*net.OpError); ok && e.Timeout() {
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
			if len(data2) < 2 {
				return IllFormedMessageError
			}

			messageType := protocol.MessageType(data2[0])
			messageHeaderSize := int(data2[1])

			if len(data2) < 2+messageHeaderSize {
				return IllFormedMessageError
			}

			switch messageType {
			case protocol.MESSAGE_REQUEST:
				var requestHeader protocol.RequestHeader

				if e := requestHeader.Unmarshal(data2[2 : 2+messageHeaderSize]); e != nil {
					return IllFormedMessageError
				}

				newPendingResultReturnCount++

				if newPendingResultReturnCount > self.incomingWindowSize {
					returnResult(self.dequeOfResultReturns, requestHeader.SequenceNumber, ErrorChannelBusy, nil)
					continue
				}

				serviceHandler, ok := self.policy.serviceHandlers[requestHeader.ServiceName]

				if !ok {
					returnResult(self.dequeOfResultReturns, requestHeader.SequenceNumber, ErrorNotImplemented, nil)
					continue
				}

				methodTable := serviceHandler.X_GetMethodTable()
				methodRecord, ok := methodTable.Search(requestHeader.MethodName)

				if !ok {
					returnResult(self.dequeOfResultReturns, requestHeader.SequenceNumber, ErrorNotImplemented, nil)
					continue
				}

				request := reflect.New(methodRecord.RequestType).Interface().(IncomingMessage)

				if e := request.Unmarshal(data2[2+messageHeaderSize:]); e != nil {
					returnResult(self.dequeOfResultReturns, requestHeader.SequenceNumber, ErrorBadRequest, nil)
					continue
				}

				backup := struct {
					holder               Channel
					logger               *logger.Logger
					id                   *uuid.UUID
					dequeOfResultReturns *deque.Deque
					sequenceNumber       int32
					serviceName          string
					methodName           string
				}{
					holder:               self.holder,
					logger:               &self.policy.Logger,
					id:                   self.id,
					dequeOfResultReturns: self.dequeOfResultReturns,
					sequenceNumber:       requestHeader.SequenceNumber,
					serviceName:          requestHeader.ServiceName,
					methodName:           requestHeader.MethodName,
				}

				self.wgOfPendingResultReturns.Add(1)

				go func() {
					response, errorCode := serviceHandler.X_InterceptMethodCall(methodRecord, context_, backup.holder, request)

					if errorCode == 0 && response == nil {
						var e error
						response, e = methodRecord.Handler(serviceHandler, context_, backup.holder, request)

						if e != nil {
							if e2, ok := e.(Error); ok && !e2.isPassive {
								errorCode = e2.code
							} else {
								backup.logger.Errorf("internal server error: id=%#v, methodID=%v, request=%#v, e=%#v", channelIDToString(backup.id), representMethodID(backup.serviceName, backup.methodName), request, e.Error())
								errorCode = ErrorInternalServer
							}
						}
					}

					returnResult(backup.dequeOfResultReturns, backup.sequenceNumber, errorCode, response)
					self.wgOfPendingResultReturns.Done()
				}()
			case protocol.MESSAGE_RESPONSE:
				var responseHeader protocol.ResponseHeader

				if e := responseHeader.Unmarshal(data2[2 : 2+messageHeaderSize]); e != nil {
					return IllFormedMessageError
				}

				if value, ok := self.pendingMethodCalls.Load(responseHeader.SequenceNumber); ok {
					self.pendingMethodCalls.Delete(responseHeader.SequenceNumber)
					completedMethodCallCount++
					methodCall_ := value.(*methodCall)

					if responseHeader.ErrorCode == 0 {
						response := reflect.New(methodCall_.responseType).Interface().(IncomingMessage)

						if e := response.Unmarshal(data2[2+messageHeaderSize:]); e != nil {
							return IllFormedMessageError
						}

						methodCall_.callback(response, 0)
					} else {
						methodCall_.callback(nil, ErrorCode(responseHeader.ErrorCode))
					}

					poolOfMethodCalls.Put(methodCall_)
				} else {
					self.policy.Logger.Warningf("ignored response: id=%#v, responseHeader=%#v", channelIDToString(self.id), responseHeader)
				}
			case protocol.MESSAGE_HEARTBEAT:
				var heartbeat protocol.Heartbeat

				if e := heartbeat.Unmarshal(data2[2 : 2+messageHeaderSize]); e != nil {
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

func (self *channelImpl) getState() channelState {
	return channelState(atomic.LoadInt32(&self.state))
}

func (self *channelImpl) getMinHeartbeatInterval() time.Duration {
	return self.timeout / 3
}

func (self *channelImpl) getSequenceNumber() int32 {
	sequenceNumber := self.nextSequenceNumber
	self.nextSequenceNumber = int32(uint32(sequenceNumber+1) & 0xFFFFFFF)
	return sequenceNumber
}

type channelState uint8

func (self channelState) GoString() string {
	switch self {
	case channelNo:
		return "<channelNo>"
	case channelNotConnected:
		return "<channelNotConnected>"
	case channelConnecting:
		return "<channelConnecting>"
	case channelConnected:
		return "<channelConnected>"
	case channelNotAccepted:
		return "<channelNotAccepted>"
	case channelAccepting:
		return "<channelAccepting>"
	case channelAccepted:
		return "<channelAccepted>"
	case channelClosed:
		return "<channelClosed>"
	default:
		return fmt.Sprintf("<channelState:%d>", self)
	}
}

type methodCall struct {
	listNode       list.ListNode
	sequenceNumber int32
	serviceName    string
	methodName     string
	request        OutgoingMessage
	responseType   reflect.Type
	autoRetry      bool
	callback       func(IncomingMessage, ErrorCode)
}

type resultReturn struct {
	listNode       list.ListNode
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
	channelNo:           ErrorChannelTimedOut,
	channelNotConnected: 0,
	channelConnecting:   0,
	channelConnected:    0,
	channelNotAccepted:  0,
	channelAccepting:    0,
	channelAccepted:     0,
	channelClosed:       ErrorChannelTimedOut,
}

func channelIDToString(channelID *uuid.UUID) string {
	if channelID == nil {
		return ""
	}

	return channelID.Base64()
}

func returnResult(dequeOfResultReturns *deque.Deque, sequenceNumber int32, errorCode ErrorCode, response OutgoingMessage) error {
	resultReturn_ := poolOfResultReturns.Get().(*resultReturn)
	resultReturn_.sequenceNumber = sequenceNumber
	resultReturn_.errorCode = errorCode
	resultReturn_.response = response
	return dequeOfResultReturns.AppendNode(nil, &resultReturn_.listNode)
}

var poolOfMethodCalls = sync.Pool{New: func() interface{} { return &methodCall{} }}
var poolOfResultReturns = sync.Pool{New: func() interface{} { return &resultReturn{} }}
