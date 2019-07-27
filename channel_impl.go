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

	"github.com/gogo/protobuf/proto"
	"github.com/let-z-go/intrusive_containers/list"
	"github.com/let-z-go/toolkit/byte_stream"
	"github.com/let-z-go/toolkit/deque"
	"github.com/let-z-go/toolkit/logger"
	"github.com/let-z-go/toolkit/semaphore"
	"github.com/let-z-go/toolkit/utils"
	"github.com/let-z-go/toolkit/uuid"

	"github.com/let-z-go/pbrpc/protocol"
)

const (
	FIFOOff FIFOMode = iota
	FIFOHalf
	FIFOFull
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
	FIFOMode           FIFOMode
	Transport          *TransportPolicy

	validateOnce sync.Once

	serviceHandlers []struct {
		key   string
		value ServiceHandler
	}

	incomingMethodInterceptors map[string][]IncomingMethodInterceptor
	outgoingMethodInterceptors map[string][]OutgoingMethodInterceptor
}

func (self *ChannelPolicy) RegisterServiceHandler(serviceHandler ServiceHandler) *ChannelPolicy {
	n := len(self.serviceHandlers)
	serviceName := serviceHandler.X_GetName()

	for i := 0; i < n; i++ {
		if self.serviceHandlers[i].key >= serviceName {
			if self.serviceHandlers[i].key > serviceName {
				self.serviceHandlers = append(self.serviceHandlers, self.serviceHandlers[n-1])

				for j := n - 1; j > i; j-- {
					self.serviceHandlers[j] = self.serviceHandlers[j-1]
				}

				self.serviceHandlers[i].key = serviceName
			}

			self.serviceHandlers[i].value = serviceHandler
			return self
		}
	}

	self.serviceHandlers = append(self.serviceHandlers, struct {
		key   string
		value ServiceHandler
	}{serviceName, serviceHandler})

	return self
}

func (self *ChannelPolicy) FindServiceHandler(serviceName string) (ServiceHandler, bool) {
	if n := len(self.serviceHandlers); n >= 1 {
		i := 0
		j := n - 1

		for i < j {
			k := (i + j) / 2

			if self.serviceHandlers[k].key < serviceName {
				i = k + 1
			} else {
				j = k
			}
		}

		if self.serviceHandlers[i].key == serviceName {
			return self.serviceHandlers[i].value, true
		}
	}

	return nil, false
}

func (self *ChannelPolicy) AddIncomingMethodInterceptor(serviceName string, methodIndex int32, incomingMethodInterceptor IncomingMethodInterceptor) *ChannelPolicy {
	if self.incomingMethodInterceptors == nil {
		self.incomingMethodInterceptors = map[string][]IncomingMethodInterceptor{}
	}

	var methodInterceptorLocator string

	if serviceName == "" {
		methodInterceptorLocator = ""
	} else if methodIndex < 0 {
		methodInterceptorLocator = serviceName
	} else {
		methodInterceptorLocator = makeMethodInterceptorLocator(serviceName, methodIndex)
	}

	self.incomingMethodInterceptors[methodInterceptorLocator] = append(self.incomingMethodInterceptors[methodInterceptorLocator], incomingMethodInterceptor)
	return self
}

func (self *ChannelPolicy) AddOutgoingMethodInterceptor(serviceName string, methodIndex int32, outgoingMethodInterceptor OutgoingMethodInterceptor) *ChannelPolicy {
	if self.outgoingMethodInterceptors == nil {
		self.outgoingMethodInterceptors = map[string][]OutgoingMethodInterceptor{}
	}

	var methodInterceptorLocator string

	if serviceName == "" {
		methodInterceptorLocator = ""
	} else if methodIndex < 0 {
		methodInterceptorLocator = serviceName
	} else {
		methodInterceptorLocator = makeMethodInterceptorLocator(serviceName, methodIndex)
	}

	self.outgoingMethodInterceptors[methodInterceptorLocator] = append(self.outgoingMethodInterceptors[methodInterceptorLocator], outgoingMethodInterceptor)
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

		if self.Transport == nil {
			self.Transport = &defaultTransportPolicy
		}
	})

	return self
}

type FIFOMode uint8

func (self FIFOMode) GoString() string {
	switch self {
	case FIFOOff:
		return "<FIFOOff>"
	case FIFOHalf:
		return "<FIFOHalf>"
	case FIFOFull:
		return "<FIFOFull>"
	default:
		return fmt.Sprintf("<FIFOMode:%d>", self)
	}
}

type IncomingMethodInterceptor func(context.Context, interface{}, IncomingMethodHandler) (OutgoingMessage, error)
type OutgoingMethodInterceptor func(context.Context, OutgoingMessage, OutgoingMethodHandler) (interface{}, error)

type IncomingMethodHandler func(context.Context, interface{}) (OutgoingMessage, error)
type OutgoingMethodHandler func(context.Context, OutgoingMessage) (interface{}, error)

type IncomingMessage interface {
	proto.Unmarshaler
}

type OutgoingMessage interface {
	proto.Sizer

	MarshalTo([]byte) (int, error)
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
var HandshakeRejectedError = errors.New("pbrpc: handshake rejected")
var IllFormedMessageError = errors.New("pbrpc: ill-formed message")

const defaultChannelTimeout = 6 * time.Second
const minChannelTimeout = 3 * time.Second
const maxChannelTimeout = 60 * time.Second
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
	asyncTaskExecutor        asyncTaskExecutor
}

func (self *channelImpl) Initialize(holder Channel, policy *ChannelPolicy, isClientSide bool) *channelImpl {
	if self.state != 0 {
		panic(errors.New("pbrpc: channel already initialized"))
	}

	self.holder = holder
	self.policy = policy.Validate()

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

func (self *channelImpl) Close() {
	self.setState(ChannelClosed)
}

func (self *channelImpl) AddListener(maxNumberOfStateChanges int) (*ChannelListener, error) {
	if self.IsClosed() {
		return nil, ChannelClosedError
	}

	self.lockOfListeners.Lock()

	if self.IsClosed() {
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

func (self *channelImpl) RemoveListener(listener *ChannelListener) error {
	if self.IsClosed() {
		return ChannelClosedError
	}

	close(listener.stateChanges)
	self.lockOfListeners.Lock()

	if self.IsClosed() {
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

func (self *channelImpl) Connect(connector Connector, context_ context.Context, serverAddress string, handshaker ClientHandshaker) error {
	self.policy.Logger.Infof("channel connection: channelID=%q, serverAddress=%#v", self.id, serverAddress)
	self.setState(ChannelConnecting)
	connection, e := connector.Connect(context_, serverAddress)

	if e != nil {
		return e
	}

	transport_ := (&transport{}).Initialize(self.policy.Transport, connection)

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

	greeter := func(context_ context.Context, handshake *[]byte) error {
		greeting.Handshake = *handshake

		if e := transport_.Write(func(byteStream *byte_stream.ByteStream) error {
			return byteStream.WriteDirectly(greeting.Size(), func(buffer []byte) error {
				_, e := greeting.MarshalTo(buffer)
				return e
			})
		}); e != nil {
			return e
		}

		if e := transport_.Flush(context_, minChannelTimeout); e != nil {
			return e
		}

		data, e := transport_.Peek(context_, minChannelTimeout)

		if e != nil {
			return e
		}

		greeting.Reset()

		if e := greeting.Unmarshal(data); e != nil {
			return e
		}

		if e := transport_.Skip(data); e != nil {
			return e
		}

		*handshake = greeting.Handshake
		return nil
	}

	if e := handshaker(self.holder.(*ClientChannel), context_, greeter); e != nil {
		transport_.Close(true)
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
		transport_.Close(true)
		return e
	}

	self.transport.Close(true)
	self.transport = *transport_
	self.setState(ChannelConnected)
	self.policy.Logger.Infof("channel establishment: serverAddress=%#v, channelID=%q, timeout=%#v, incomingWindowSize=%#v, outgoingWindowSize=%#v",
		serverAddress,
		self.id,
		self.timeout/time.Millisecond,
		self.incomingWindowSize,
		self.outgoingWindowSize)
	return nil
}

func (self *channelImpl) Accept(context_ context.Context, serverID int32, connection net.Conn, handshaker ServerHandshaker) error {
	clientAddress := connection.RemoteAddr()
	self.policy.Logger.Infof("channel acceptance: serverID=%#v, clientAddress=%q", serverID, clientAddress)
	self.setState(ChannelAccepting)
	transport_ := (&transport{}).Initialize(self.policy.Transport, connection)
	data, e := transport_.Peek(context_, minChannelTimeout)

	if e != nil {
		transport_.Close(true)
		return e
	}

	var greeting protocol.Greeting

	if e := greeting.Unmarshal(data); e != nil {
		transport_.Close(true)
		return e
	}

	if e := transport_.Skip(data); e != nil {
		transport_.Close(true)
		return e
	}

	var id uuid.UUID

	if len(greeting.Channel.Id) == 0 {
		id, e = uuid.GenerateUUID4()

		if e != nil {
			transport_.Close(true)
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

	handshakeIsAccepted, e := handshaker(self.holder.(*ServerChannel), context_, &greeting.Handshake)

	if e != nil {
		transport_.Close(true)
		return e
	}

	if e := transport_.Write(func(byteStream *byte_stream.ByteStream) error {
		return byteStream.WriteDirectly(greeting.Size(), func(buffer []byte) error {
			_, e := greeting.MarshalTo(buffer)
			return e
		})
	}); e != nil {
		transport_.Close(true)
		return e
	}

	if e := transport_.Flush(context_, minChannelTimeout); e != nil {
		transport_.Close(true)
		return e
	}

	if !handshakeIsAccepted {
		transport_.Close(false)
		return HandshakeRejectedError
	}

	self.id = id
	self.timeout = time.Duration(greeting.Channel.Timeout) * time.Millisecond
	self.incomingWindowSize = greeting.Channel.OutgoingWindowSize
	self.outgoingWindowSize = greeting.Channel.IncomingWindowSize

	if e := self.dequeOfMethodCalls.CommitNodeRemovals(self.outgoingWindowSize - minChannelWindowSize); e != nil {
		transport_.Close(true)
		return e
	}

	self.transport.Close(true)
	self.transport = *transport_
	self.setState(ChannelAccepted)
	self.policy.Logger.Infof("channel establishment: serverID=%#v, clientAddress=%q, channelID=%q, timeout=%#v, incomingWindowSize=%#v, outgoingWindowSize=%#v",
		serverID,
		clientAddress,
		self.id,
		self.timeout/time.Millisecond,
		self.incomingWindowSize,
		self.outgoingWindowSize)
	return nil
}

func (self *channelImpl) Dispatch(context_ context.Context, cancel context.CancelFunc, serverID int32) error {
	if state := self.getState(); state != ChannelConnected && state != ChannelAccepted {
		panic(&invalidChannelStateError{fmt.Sprintf("state=%#v", state)})
	}

	error_ := make(chan error, 1)

	go func() {
		error_ <- self.sendMessages(context_, cancel)
	}()

	e := self.receiveMessages(context_, serverID)
	cancel()

	if e2 := <-error_; e2 != context_.Err() {
		e = e2
	}

	return e
}

func (self *channelImpl) CallMethod(
	context_ context.Context,
	contextVars_ *ContextVars,
	request OutgoingMessage,
	responseType reflect.Type,
	autoRetryMethodCall bool,
	callback func(interface{}, ErrorCode, string, []byte),
) error {
	if self.IsClosed() {
		errorCode := self.getErrorCode()
		return &Error{code: errorCode}
	}

	methodCall_ := poolOfMethodCalls.Get().(*methodCall)
	methodCall_.RequestID = contextVars_.RequestID
	methodCall_.ServiceName = contextVars_.ServiceName
	methodCall_.MethodName = contextVars_.MethodName
	methodCall_.ResourceID = contextVars_.ResourceID
	methodCall_.ExtraData = contextVars_.ExtraData
	methodCall_.Request = request
	methodCall_.ResponseType = responseType
	methodCall_.AutoRetry = autoRetryMethodCall
	methodCall_.Callback = callback

	if e := self.dequeOfMethodCalls.AppendNode(context_, &methodCall_.ListNode); e != nil {
		if e == semaphore.SemaphoreClosedError {
			errorCode := self.getErrorCode()
			e = &Error{code: errorCode}
		}

		return e
	}

	return nil
}

func (self *channelImpl) IsClosed() bool {
	return self.getErrorCode() != 0
}

func (self *channelImpl) GetTimeout() time.Duration {
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
			panic(&invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case ChannelConnecting:
		switch newState {
		case ChannelConnecting:
			return
		case ChannelConnected:
		case ChannelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(&invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case ChannelConnected:
		switch newState {
		case ChannelConnecting:
			errorCode = ErrorChannelBroken
		case ChannelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(&invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case ChannelNotAccepted:
		switch newState {
		case ChannelAccepting:
		default:
			panic(&invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case ChannelAccepting:
		switch newState {
		case ChannelAccepted:
		case ChannelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(&invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	case ChannelAccepted:
		switch newState {
		case ChannelClosed:
			errorCode = ErrorChannelBroken
		default:
			panic(&invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
		}
	default:
		panic(&invalidChannelStateError{fmt.Sprintf("oldState=%#v, newState=%#v", oldState, newState)})
	}

	atomic.StoreInt32(&self.state, int32(newState))
	self.policy.Logger.Infof("channel state change: channelID=%q, oldState=%#v, newState=%#v", self.id, oldState, newState)
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

				if methodCallsAreRetriable && methodCall_.AutoRetry {
					list_.AppendNode(&methodCall_.ListNode)
					retriedMethodCallCount++
				} else {
					methodCall_.Callback(nil, errorCode, "", nil)
					completedMethodCallCount++
					poolOfMethodCalls.Put(methodCall_)
				}

				return true
			})

			list_.Sort(func(listNode1 *list.ListNode, listNode2 *list.ListNode) bool {
				methodCall1 := (*methodCall)(listNode1.GetContainer(unsafe.Offsetof(methodCall{}.ListNode)))
				methodCall2 := (*methodCall)(listNode2.GetContainer(unsafe.Offsetof(methodCall{}.ListNode)))
				x := methodCall1.SequenceNumber
				y := methodCall2.SequenceNumber
				z := int32(utils.Abs(int64(x-y))) & 0x40000000
				x ^= z
				y ^= z
				return x < y
			})

			self.dequeOfMethodCalls.DiscardNodeRemovals(list_, retriedMethodCallCount)
			self.dequeOfMethodCalls.CommitNodeRemovals(completedMethodCallCount)
			atomic.StoreInt32(&self.pendingResultReturnCount, 0)
			list_.Initialize()
			self.dequeOfResultReturns.Close(list_)
			self.dequeOfResultReturns = (&deque.Deque{}).Initialize(math.MaxInt32)
			getListNode := list_.GetNodesSafely()

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				listNode.Reset()
				resultReturn_ := (*resultReturn)(listNode.GetContainer(unsafe.Offsetof(resultReturn{}.ListNode)))
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

			self.transport.Close(false)

			{
				list_ := (&list.List{}).Initialize()
				self.dequeOfMethodCalls.Close(list_)
				getListNode := list_.GetNodesSafely()

				for listNode := getListNode(); listNode != nil; listNode = getListNode() {
					listNode.Reset()
					methodCall_ := (*methodCall)(listNode.GetContainer(unsafe.Offsetof(methodCall{}.ListNode)))
					methodCall_.Callback(nil, errorCode2, "", nil)
					poolOfMethodCalls.Put(methodCall_)
				}
			}

			self.pendingMethodCalls.Range(func(key interface{}, value interface{}) bool {
				self.pendingMethodCalls.Delete(key)
				methodCall_ := value.(*methodCall)

				if methodCallsAreRetriable && methodCall_.AutoRetry {
					methodCall_.Callback(nil, errorCode2, "", nil)
				} else {
					methodCall_.Callback(nil, errorCode, "", nil)
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
					listNode.Reset()
					resultReturn_ := (*resultReturn)(listNode.GetContainer(unsafe.Offsetof(resultReturn{}.ListNode)))
					poolOfResultReturns.Put(resultReturn_)
				}
			}

			self.wgOfPendingResultReturns.Wait()
		}
	}
}

func (self *channelImpl) sendMessages(context_ context.Context, cancel context.CancelFunc) error {
	errors_ := make(chan error, 2)

	type Task struct {
		List              *list.List
		NumberOfListNodes int32
	}

	tasksOfMethodCalls := make(chan Task)
	tasksOfResultReturns := make(chan Task)

	go func() {
		list1 := (&list.List{}).Initialize()
		list2 := (&list.List{}).Initialize()

		for {
			numberOfListNodes, e := self.dequeOfMethodCalls.RemoveAllNodes(context_, false, list1)

			if e != nil {
				errors_ <- e
				return
			}

			select {
			case tasksOfMethodCalls <- Task{list1, numberOfListNodes}:
			case <-context_.Done():
				self.dequeOfMethodCalls.DiscardNodeRemovals(list1, numberOfListNodes)
				errors_ <- context_.Err()
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
			numberOfListNodes, e := self.dequeOfResultReturns.RemoveAllNodes(context_, true, list1)

			if e != nil {
				errors_ <- e
				return
			}

			select {
			case tasksOfResultReturns <- Task{list1, numberOfListNodes}:
			case <-context_.Done():
				errors_ <- context_.Err()
				return
			}

			list1, list2 = list2, list1
			list1.Initialize()
		}
	}()

	var taskOfMethodCalls Task
	var taskOfResultReturns Task

	cleanup := func(ok bool) {
		if taskOfMethodCalls.List != nil {
			if ok {
				getListNode := taskOfMethodCalls.List.GetNodesSafely()

				for listNode := getListNode(); listNode != nil; listNode = getListNode() {
					listNode.Reset()
					methodCall_ := (*methodCall)(listNode.GetContainer(unsafe.Offsetof(methodCall{}.ListNode)))

					if !methodCall_.AutoRetry {
						methodCall_.ServiceName = ""
						methodCall_.MethodName = ""
						methodCall_.ResourceID = ""
						methodCall_.ExtraData = nil
						methodCall_.Request = nil
					}

					self.pendingMethodCalls.Store(methodCall_.SequenceNumber, methodCall_)
				}
			} else {
				self.dequeOfMethodCalls.DiscardNodeRemovals(taskOfMethodCalls.List, taskOfMethodCalls.NumberOfListNodes)
			}
		}

		if taskOfResultReturns.List != nil {
			getListNode := taskOfResultReturns.List.GetNodesSafely()

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				listNode.Reset()
				resultReturn_ := (*resultReturn)(listNode.GetContainer(unsafe.Offsetof(resultReturn{}.ListNode)))
				poolOfResultReturns.Put(resultReturn_)
			}

			if ok {
				atomic.AddInt32(&self.pendingResultReturnCount, -taskOfResultReturns.NumberOfListNodes)
			}
		}

		if !ok {
			cancel()
			<-errors_
			<-errors_
		}
	}

	for {
		taskOfMethodCalls = Task{nil, 0}
		taskOfResultReturns = Task{nil, 0}

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
			select {
			case taskOfMethodCalls = <-tasksOfMethodCalls:
			case taskOfResultReturns = <-tasksOfResultReturns:
			default:
			}
		}

		if taskOfMethodCalls.List != nil {
			getListNode := taskOfMethodCalls.List.GetNodesSafely()
			completedMethodCallCount := int32(0)

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				methodCall_ := (*methodCall)(listNode.GetContainer(unsafe.Offsetof(methodCall{}.ListNode)))
				methodCall_.SequenceNumber = self.getSequenceNumber()

				requestHeader := protocol.RequestHeader{
					SequenceNumber: methodCall_.SequenceNumber,
					Id:             methodCall_.RequestID[:],
					ServiceName:    methodCall_.ServiceName,
					MethodName:     methodCall_.MethodName,
					ResourceId:     methodCall_.ResourceID,
					ExtraData:      methodCall_.ExtraData,
				}

				requestHeaderSize := requestHeader.Size()

				if e := self.transport.Write(func(byteStream *byte_stream.ByteStream) error {
					return byteStream.WriteDirectly(3+requestHeaderSize+methodCall_.Request.Size(), func(buffer []byte) error {
						buffer[0] = uint8(protocol.MESSAGE_REQUEST)
						buffer[1] = uint8(requestHeaderSize >> 8)
						buffer[2] = uint8(requestHeaderSize)

						if _, e := requestHeader.MarshalTo(buffer[3:]); e != nil {
							return e
						}

						if _, e := methodCall_.Request.MarshalTo(buffer[3+requestHeaderSize:]); e != nil {
							return e
						}

						return nil
					})
				}); e != nil {
					if e == PacketPayloadTooLargeError {
						listNode.Remove()
						listNode.Reset()
						taskOfMethodCalls.NumberOfListNodes--
						methodCall_.Callback(nil, ErrorPacketPayloadTooLarge, "", nil)
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

		if taskOfResultReturns.List != nil {
			getListNode := taskOfResultReturns.List.GetNodes()

			for listNode := getListNode(); listNode != nil; listNode = getListNode() {
				resultReturn_ := (*resultReturn)(listNode.GetContainer(unsafe.Offsetof(resultReturn{}.ListNode)))

				responseHeader := protocol.ResponseHeader{
					SequenceNumber: resultReturn_.SequenceNumber,
					ErrorCode:      int32(resultReturn_.ErrorCode),
					ErrorDesc:      resultReturn_.ErrorDesc,
				}

				responseHeaderSize := responseHeader.Size()

				if e := self.transport.Write(func(byteStream *byte_stream.ByteStream) error {
					return byteStream.WriteDirectly(3+responseHeaderSize+resultReturn_.Response.Size(), func(buffer []byte) error {
						buffer[0] = uint8(protocol.MESSAGE_RESPONSE)
						buffer[1] = uint8(responseHeaderSize >> 8)
						buffer[2] = uint8(responseHeaderSize)

						if _, e := responseHeader.MarshalTo(buffer[3:]); e != nil {
							return e
						}

						if _, e := resultReturn_.Response.MarshalTo(buffer[3+responseHeaderSize:]); e != nil {
							return e
						}

						return nil
					})
				}); e != nil {
					cleanup(false)
					return e
				}
			}
		}

		if taskOfMethodCalls.List == nil && taskOfResultReturns.List == nil {
			heartbeat := protocol.Heartbeat{}
			heartbeatSize := heartbeat.Size()

			if e := self.transport.Write(func(byteStream *byte_stream.ByteStream) error {
				return byteStream.WriteDirectly(3+heartbeatSize, func(buffer []byte) error {
					buffer[0] = uint8(protocol.MESSAGE_HEARTBEAT)
					buffer[1] = uint8(heartbeatSize >> 8)
					buffer[2] = uint8(heartbeatSize)
					_, e := heartbeat.MarshalTo(buffer[3:])
					return e
				})
			}); e != nil {
				cleanup(false)
				return e
			}
		}

		cleanup(true)

		if e := self.transport.Flush(context_, 0); e != nil {
			cancel()
			<-errors_
			<-errors_
			return e
		}
	}
}

func (self *channelImpl) receiveMessages(context_ context.Context, serverID int32) error {
	for {
		data, e := self.transport.PeekInBatch(context_, 2*self.getMinHeartbeatInterval())

		if e != nil {
			return e
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

				if e := self.receiveRequest(context_, serverID, &requestHeader, messagePayload, &newPendingResultReturnCount); e != nil {
					return e
				}
			case protocol.MESSAGE_RESPONSE:
				var responseHeader protocol.ResponseHeader

				if e := responseHeader.Unmarshal(messageHeader); e != nil {
					return IllFormedMessageError
				}

				if e := self.receiveResponse(context_, serverID, &responseHeader, messagePayload, &completedMethodCallCount); e != nil {
					return e
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

		if e := self.transport.SkipInBatch(data); e != nil {
			return e
		}
	}
}

func (self *channelImpl) receiveRequest(context_ context.Context, serverID int32, requestHeader *protocol.RequestHeader, requestPayload []byte, pendingResultReturnCount *int32) error {
	contextVars_ := ContextVars{
		ServerID:    serverID,
		Channel:     self.holder,
		RequestID:   uuid.UUIDFromBytes(requestHeader.Id),
		ServiceName: requestHeader.ServiceName,
		MethodName:  requestHeader.MethodName,
		MethodIndex: -1,
		ResourceID:  requestHeader.ResourceId,
		ExtraData:   requestHeader.ExtraData,

		logger: &self.policy.Logger,
	}

	incomingMethodInterceptors := poolOfIncomingMethodInterceptors.Get().([]IncomingMethodInterceptor)[:0]
	incomingMethodInterceptors = append(incomingMethodInterceptors, self.policy.incomingMethodInterceptors[""]...)
	incomingMethodInterceptors = append(incomingMethodInterceptors, self.policy.incomingMethodInterceptors[contextVars_.ServiceName]...)
	*pendingResultReturnCount++

	if *pendingResultReturnCount > self.incomingWindowSize {
		return self.rejectRequest(requestPayload, incomingMethodInterceptors, ErrorTooManyRequests, context_, &contextVars_, requestHeader.SequenceNumber)
	}

	serviceHandler, ok := self.policy.FindServiceHandler(contextVars_.ServiceName)

	if !ok {
		return self.rejectRequest(requestPayload, incomingMethodInterceptors, ErrorNotFound, context_, &contextVars_, requestHeader.SequenceNumber)
	}

	methodTable := serviceHandler.X_GetMethodTable()
	methodRecord, ok := methodTable.Search(contextVars_.MethodName)

	if !ok {
		return self.rejectRequest(requestPayload, incomingMethodInterceptors, ErrorNotFound, context_, &contextVars_, requestHeader.SequenceNumber)
	}

	contextVars_.MethodIndex = methodRecord.Index
	methodInterceptorLocator := makeMethodInterceptorLocator(contextVars_.ServiceName, contextVars_.MethodIndex)
	incomingMethodInterceptors = append(incomingMethodInterceptors, self.policy.incomingMethodInterceptors[methodInterceptorLocator]...)
	request := reflect.New(methodRecord.RequestType).Interface().(IncomingMessage)

	if e := request.Unmarshal(requestPayload); e != nil {
		return self.rejectRequest(requestPayload, incomingMethodInterceptors, ErrorBadRequest, context_, &contextVars_, requestHeader.SequenceNumber)
	}

	return self.acceptRequest(incomingMethodInterceptors, methodRecord, serviceHandler, context_, &contextVars_, request, requestHeader.SequenceNumber)
}

func (self *channelImpl) rejectRequest(
	requestPayload []byte,
	incomingMethodInterceptors []IncomingMethodInterceptor,
	errorCode ErrorCode,
	context_ context.Context,
	contextVars_ *ContextVars,
	sequenceNumber int32,
) error {
	if len(incomingMethodInterceptors) == 0 {
		poolOfIncomingMethodInterceptors.Put(incomingMethodInterceptors)
		returnResult(sequenceNumber, errorCode, X_MustGetErrorDesc(errorCode), RawMessage(nil), self.dequeOfResultReturns)
		return nil
	}

	var request RawMessage
	request.Unmarshal(requestPayload)
	self.wgOfPendingResultReturns.Add(1)

	f := func(
		incomingMethodInterceptors []IncomingMethodInterceptor,
		errorCode ErrorCode,
		context_ context.Context,
		contextVars_ ContextVars,
		request RawMessage,
		logger_ *logger.Logger,
		sequenceNumber int32,
		dequeOfResultReturns *deque.Deque,
		self *channelImpl,
	) func() {
		return func() {
			n := len(incomingMethodInterceptors)
			i := 0
			var incomingMethodHandler IncomingMethodHandler

			incomingMethodHandler = func(context_ context.Context, request interface{}) (OutgoingMessage, error) {
				if i == n {
					poolOfIncomingMethodInterceptors.Put(incomingMethodInterceptors)
					return nil, X_MakeError(errorCode, "", nil)
				} else {
					incomingMethodInterceptor := incomingMethodInterceptors[i]
					i++
					return incomingMethodInterceptor(context_, request, incomingMethodHandler)
				}
			}

			response, e := incomingMethodHandler(bindContextVars(context_, &contextVars_), &request)
			var errorCode2 ErrorCode
			var errorDesc2 string
			response, errorCode2, errorDesc2 = parseError(response, e, logger_, &contextVars_, &request)
			returnResult(sequenceNumber, errorCode2, errorDesc2, response, dequeOfResultReturns)
			self.wgOfPendingResultReturns.Done()
		}
	}(
		incomingMethodInterceptors,
		errorCode,
		context_,
		*contextVars_,
		request,
		&self.policy.Logger,
		sequenceNumber,
		self.dequeOfResultReturns,
		self,
	)

	switch self.policy.FIFOMode {
	case FIFOHalf:
		if contextVars_.ResourceID == "" {
			go f()
		} else {
			resourceKey := makeResourceKey(contextVars_.ServiceName, contextVars_.ResourceID)

			if e := self.asyncTaskExecutor.ExecuteAsyncTask(context_, resourceKey, f); e != nil {
				self.wgOfPendingResultReturns.Done()
				return e
			}
		}
	case FIFOFull:
		if e := self.asyncTaskExecutor.ExecuteAsyncTask(context_, "", f); e != nil {
			self.wgOfPendingResultReturns.Done()
			return e
		}
	default:
		go f()
	}

	return nil
}

func (self *channelImpl) acceptRequest(
	incomingMethodInterceptors []IncomingMethodInterceptor,
	methodRecord *MethodRecord,
	serviceHandler ServiceHandler,
	context_ context.Context,
	contextVars_ *ContextVars,
	request interface{},
	sequenceNumber int32,
) error {
	self.wgOfPendingResultReturns.Add(1)

	f := func(
		incomingMethodInterceptors []IncomingMethodInterceptor,
		methodRecord *MethodRecord,
		serviceHandler ServiceHandler,
		context_ context.Context,
		contextVars_ ContextVars,
		request interface{},
		logger_ *logger.Logger,
		sequenceNumber int32,
		dequeOfResultReturns *deque.Deque,
		self *channelImpl,
	) func() {
		return func() {
			var response OutgoingMessage
			var e error

			if n := len(incomingMethodInterceptors); n == 0 {
				poolOfIncomingMethodInterceptors.Put(incomingMethodInterceptors)
				response, e = methodRecord.Handler(serviceHandler, bindContextVars(context_, &contextVars_), request)
			} else {
				i := 0
				var incomingMethodHandler IncomingMethodHandler

				incomingMethodHandler = func(context_ context.Context, request interface{}) (OutgoingMessage, error) {
					if i == n {
						poolOfIncomingMethodInterceptors.Put(incomingMethodInterceptors)
						return methodRecord.Handler(serviceHandler, context_, request)
					} else {
						incomingMethodInterceptor := incomingMethodInterceptors[i]
						i++
						return incomingMethodInterceptor(context_, request, incomingMethodHandler)
					}
				}

				response, e = incomingMethodHandler(bindContextVars(context_, &contextVars_), request)
			}

			var errorCode2 ErrorCode
			var errorDesc2 string
			response, errorCode2, errorDesc2 = parseError(response, e, logger_, &contextVars_, request)
			returnResult(sequenceNumber, errorCode2, errorDesc2, response, dequeOfResultReturns)
			self.wgOfPendingResultReturns.Done()
		}
	}(
		incomingMethodInterceptors,
		methodRecord,
		serviceHandler,
		context_,
		*contextVars_,
		request,
		&self.policy.Logger,
		sequenceNumber,
		self.dequeOfResultReturns,
		self,
	)

	switch self.policy.FIFOMode {
	case FIFOHalf:
		if contextVars_.ResourceID == "" {
			go f()
		} else {
			resourceKey := makeResourceKey(contextVars_.ServiceName, contextVars_.ResourceID)

			if e := self.asyncTaskExecutor.ExecuteAsyncTask(context_, resourceKey, f); e != nil {
				self.wgOfPendingResultReturns.Done()
				return e
			}
		}
	case FIFOFull:
		if e := self.asyncTaskExecutor.ExecuteAsyncTask(context_, "", f); e != nil {
			self.wgOfPendingResultReturns.Done()
			return e
		}
	default:
		go f()
	}

	return nil
}

func (self *channelImpl) receiveResponse(context_ context.Context, serverID int32, responseHeader *protocol.ResponseHeader, responsePayload []byte, completedMethodCallCount *int32) error {
	value, ok := self.pendingMethodCalls.Load(responseHeader.SequenceNumber)

	if !ok {
		self.policy.Logger.Warningf("ignored response: serverID=%#v, channelID=%q, responseHeader=%q", serverID, self.id, responseHeader)
		return nil
	}

	self.pendingMethodCalls.Delete(responseHeader.SequenceNumber)
	methodCall_ := value.(*methodCall)

	if responseHeader.ErrorCode == 0 {
		response := reflect.New(methodCall_.ResponseType).Interface().(IncomingMessage)

		if e := response.Unmarshal(responsePayload); e != nil {
			return IllFormedMessageError
		}

		methodCall_.Callback(response, 0, "", nil)
	} else {
		var errorData []byte

		if len(responsePayload) == 0 {
			errorData = nil
		} else {
			errorData = responsePayload
		}

		methodCall_.Callback(nil, ErrorCode(responseHeader.ErrorCode), responseHeader.ErrorDesc, errorData)
	}

	*completedMethodCallCount++
	poolOfMethodCalls.Put(methodCall_)
	return nil
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
	self.nextSequenceNumber = int32((uint32(sequenceNumber) + 1) & 0x7FFFFFFF)
	return sequenceNumber
}

type methodCall struct {
	ListNode       list.ListNode
	SequenceNumber int32
	RequestID      uuid.UUID
	ServiceName    string
	MethodName     string
	ResourceID     string
	ExtraData      map[string][]byte
	Request        OutgoingMessage
	ResponseType   reflect.Type
	AutoRetry      bool
	Callback       func(interface{}, ErrorCode, string, []byte)
}

type resultReturn struct {
	ListNode       list.ListNode
	SequenceNumber int32
	ErrorCode      ErrorCode
	ErrorDesc      string
	Response       OutgoingMessage
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

type contextVars struct{}

var defaultTransportPolicy TransportPolicy

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
var poolOfIncomingMethodInterceptors = sync.Pool{New: func() interface{} { return make([]IncomingMethodInterceptor, normalNumberOfMethodInterceptors) }}

func makeResourceKey(serviceName string, resourceID string) string {
	return serviceName + ":" + resourceID
}

func parseError(response OutgoingMessage, e error, logger *logger.Logger, contextVars_ *ContextVars, request interface{}) (OutgoingMessage, ErrorCode, string) {
	if e == nil {
		return response, 0, ""
	} else {
		if e2, ok := e.(*Error); ok && e2.IsMade() {
			return RawMessage(e2.GetData()), e2.code, e2.GetDesc()
		} else {
			logger.Errorf(
				"internal server error: requestID=%q, serviceName=%#v, methodName=%#v, resourceID=%#v, request=%q, e=%q",
				contextVars_.RequestID,
				contextVars_.ServiceName,
				contextVars_.MethodName,
				contextVars_.ResourceID,
				request,
				e,
			)

			return RawMessage(nil), ErrorInternalServer, X_MustGetErrorDesc(ErrorInternalServer)
		}
	}
}

func returnResult(sequenceNumber int32, errorCode ErrorCode, errorDesc string, response OutgoingMessage, dequeOfResultReturns *deque.Deque) error {
	resultReturn_ := poolOfResultReturns.Get().(*resultReturn)
	resultReturn_.SequenceNumber = sequenceNumber
	resultReturn_.ErrorCode = errorCode
	resultReturn_.ErrorDesc = errorDesc
	resultReturn_.Response = response
	return dequeOfResultReturns.AppendNode(nil, &resultReturn_.ListNode)
}
