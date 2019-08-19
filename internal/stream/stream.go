package stream

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/let-z-go/intrusives/list"
	"github.com/let-z-go/toolkit/deque"
	"github.com/let-z-go/toolkit/uuid"

	"github.com/let-z-go/pbrpc/internal/protocol"
	"github.com/let-z-go/pbrpc/internal/transport"
)

type Stream struct {
	stream

	localConcurrency        int32
	deques                  [2]deque.Deque
	dequeOfPendingRequests  *deque.Deque
	dequeOfPendingResponses *deque.Deque
	isHungUp                int32
	pendingHangup           chan *protocol.Hangup
}

func (self *Stream) Init(options *Options, dequeOfPendingRequests *deque.Deque, dequeOfPendingResponses *deque.Deque) *Stream {
	self.options = options.Normalize()
	self.transport.Init(options.Transport)

	if dequeOfPendingRequests == nil {
		dequeOfPendingRequests = self.deques[0].Init(0)
	}

	if dequeOfPendingResponses == nil {
		dequeOfPendingResponses = self.deques[1].Init(0)
	}

	self.dequeOfPendingRequests = dequeOfPendingRequests
	self.dequeOfPendingResponses = dequeOfPendingResponses
	self.pendingHangup = make(chan *protocol.Hangup, 1)
	return self
}

func (self *Stream) Close() error {
	err := self.transport.Close()

	if self.dequeOfPendingRequests == &self.deques[0] {
		listOfPendingRequests := new(list.List).Init()
		self.dequeOfPendingRequests.Close(listOfPendingRequests)
		PutPooledPendingRequests(listOfPendingRequests)
	}

	if self.dequeOfPendingResponses == &self.deques[1] {
		listOfPendingResponses := new(list.List).Init()
		self.dequeOfPendingResponses.Close(listOfPendingResponses)
		PutPooledPendingResponses(listOfPendingResponses)
	}

	return err
}

func (self *Stream) Accept(ctx context.Context, connection net.Conn, handshaker Handshaker) (bool, error) {
	ok, err := self.transport.Accept(ctx, connection, &serverHandshaker{
		Inner: handshaker,

		stream: &self.stream,
	})

	if err != nil {
		return false, err
	}

	if err := self.adjust(); err != nil {
		return false, err
	}

	return ok, nil
}

func (self *Stream) Connect(ctx context.Context, serverAddress string, transportID uuid.UUID, handshaker Handshaker) (bool, error) {
	ok, err := self.transport.Connect(ctx, serverAddress, transportID, &clientHandshaker{
		Inner: handshaker,

		stream: &self.stream,
	})

	if err != nil {
		return false, err
	}

	if err := self.adjust(); err != nil {
		return false, err
	}

	return ok, nil
}

func (self *Stream) Process(ctx context.Context, messageProcessor MessageProcessor) error {
	errs := make(chan error, 2)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		errs <- self.sendPackets(ctx, messageProcessor)
		cancel()
	}()

	errs <- self.receivePackets(ctx, messageProcessor, messageProcessor)
	cancel()
	err := <-errs
	<-errs
	return err
}

func (self *Stream) SendRequest(ctx context.Context, requestHeader *protocol.RequestHeader, request Message) error {
	pendingRequest := pendingRequestPool.Get().(*PendingRequest)
	pendingRequest.Header = *requestHeader
	pendingRequest.Payload = request

	if err := self.dequeOfPendingRequests.AppendNode(ctx, &pendingRequest.ListNode); err != nil {
		pendingRequestPool.Put(pendingRequest)

		switch err {
		case deque.ErrDequeClosed:
			return ErrClosed
		default:
			return err
		}
	}

	return nil
}

func (self *Stream) SendResponse(responseHeader *protocol.ResponseHeader, response Message) error {
	pendingResponse := pendingResponsePool.Get().(*PendingResponse)
	pendingResponse.Header = *responseHeader
	pendingResponse.Payload = response

	if err := self.dequeOfPendingResponses.AppendNode(context.Background(), &pendingResponse.ListNode); err != nil {
		pendingResponsePool.Put(pendingResponse)

		switch err {
		case deque.ErrDequeClosed:
			return ErrClosed
		default:
			return err
		}
	}

	return nil
}

func (self *Stream) Abort() {
	if atomic.CompareAndSwapInt32(&self.isHungUp, 0, 1) {
		self.pendingHangup <- &protocol.Hangup{
			ErrorCode: ErrorAborted,
		}
	}
}

func (self *Stream) adjust() error {
	{
		n := self.remoteConcurrencyLimit - self.dequeOfPendingRequests.GetMaxLength()

		if n < 0 {
			return ErrConcurrencyOverflow
		}

		self.dequeOfPendingRequests.CommitNodesRemoval(n)
	}

	{
		n := self.localConcurrencyLimit - self.dequeOfPendingResponses.GetMaxLength()

		if n < 0 {
			return ErrConcurrencyOverflow
		}

		self.dequeOfPendingResponses.CommitNodesRemoval(n)
	}

	return nil
}

func (self *Stream) receivePackets(ctx context.Context, messageFactory MessageFactory, messageHandler MessageHandler) error {
	var packet Packet

	for {
		if err := self.peek(ctx, self.incomingKeepaliveInterval/2*3, messageFactory, &packet); err != nil {
			return err
		}

		oldLocalConcurrency := int(atomic.LoadInt32(&self.localConcurrency))
		newLocalConcurrency := oldLocalConcurrency
		handledResponseCount := 0

		if err := self.handlePacket(
			ctx,
			&packet,
			messageHandler,
			&newLocalConcurrency,
			&handledResponseCount,
		); err != nil {
			return err
		}

		for {
			ok, err := self.peekNext(ctx, messageFactory, &packet)

			if err != nil {
				return err
			}

			if !ok {
				break
			}

			if err := self.handlePacket(
				ctx,
				&packet,
				messageHandler,
				&newLocalConcurrency,
				&handledResponseCount,
			); err != nil {
				return err
			}
		}

		atomic.AddInt32(&self.localConcurrency, int32(newLocalConcurrency-oldLocalConcurrency))
		self.dequeOfPendingRequests.CommitNodesRemoval(handledResponseCount)
	}
}

func (self *Stream) peek(ctx context.Context, timeout time.Duration, messageFactory MessageFactory, packet *Packet) error {
	var transportPacket transport.Packet

	if err := self.transport.Peek(ctx, timeout, &transportPacket); err != nil {
		return err
	}

	if err := self.parseTransportPacket(ctx, &transportPacket, messageFactory, packet); err != nil {
		return err
	}

	return nil
}

func (self *Stream) peekNext(ctx context.Context, messageFactory MessageFactory, packet *Packet) (bool, error) {
	var transportPacket transport.Packet
	ok, err := self.transport.PeekNext(&transportPacket)

	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	if err := self.parseTransportPacket(ctx, &transportPacket, messageFactory, packet); err != nil {
		return false, err
	}

	return true, nil
}

func (self *Stream) parseTransportPacket(ctx context.Context, transportPacket *transport.Packet, messageFactory MessageFactory, packet *Packet) error {
	packet.MessageType = transportPacket.Header.MessageType

	switch packet.MessageType {
	case protocol.MESSAGE_KEEPALIVE:
		packet.Message = nil
		packet.Err = nil
		messageFactory.NewKeepalive(packet)

		if packet.Err == nil {
			packet.Err = packet.Message.Unmarshal(transportPacket.Payload)
		}
	case protocol.MESSAGE_REQUEST:
		rawPacket := transportPacket.Payload
		packetSize := len(rawPacket)

		if packetSize < 4 {
			return self.hangUp(ctx, ErrorBadIncomingPacket)
		}

		requestHeaderSize := int(int32(binary.BigEndian.Uint32(rawPacket)))
		requestOffset := 4 + requestHeaderSize

		if requestHeaderSize < 0 || requestOffset > packetSize {
			return self.hangUp(ctx, ErrorBadIncomingPacket)
		}

		packet.RequestHeader.Reset()

		if packet.RequestHeader.Unmarshal(rawPacket[4:requestOffset]) != nil {
			return self.hangUp(ctx, ErrorBadIncomingPacket)
		}

		packet.Message = nil
		packet.Err = nil
		messageFactory.NewRequest(packet)

		if packet.Err == nil {
			packet.Err = packet.Message.Unmarshal(rawPacket[requestOffset:])
		}
	case protocol.MESSAGE_RESPONSE:
		rawPacket := transportPacket.Payload
		packetSize := len(rawPacket)

		if packetSize < 4 {
			return self.hangUp(ctx, ErrorBadIncomingPacket)
		}

		responseHeaderSize := int(int32(binary.BigEndian.Uint32(rawPacket)))
		responseOffset := 4 + responseHeaderSize

		if responseHeaderSize < 0 || responseOffset > packetSize {
			return self.hangUp(ctx, ErrorBadIncomingPacket)
		}

		packet.ResponseHeader.Reset()

		if packet.ResponseHeader.Unmarshal(rawPacket[4:responseOffset]) != nil {
			return self.hangUp(ctx, ErrorBadIncomingPacket)
		}

		packet.Message = nil
		packet.Err = nil
		messageFactory.NewResponse(packet)

		if packet.Err == nil {
			packet.Err = packet.Message.Unmarshal(rawPacket[responseOffset:])
		}
	case protocol.MESSAGE_HANGUP:
		if packet.hangup.Unmarshal(transportPacket.Payload) != nil {
			return self.hangUp(ctx, ErrorBadIncomingPacket)
		}
	default:
		return self.hangUp(ctx, ErrorBadIncomingPacket)
	}

	return nil
}

func (self *Stream) handlePacket(
	ctx context.Context,
	packet *Packet,
	messageHandler MessageHandler,
	localConcurrency *int,
	handledResponseCount *int,
) error {
	switch packet.MessageType {
	case protocol.MESSAGE_KEEPALIVE:
		messageHandler.HandleKeepalive(ctx, packet)
	case protocol.MESSAGE_REQUEST:
		if packet.Err == ErrPacketDropped {
			return nil
		}

		if *localConcurrency == self.localConcurrencyLimit {
			return self.hangUp(ctx, ErrorTooManyIncomingRequests)
		}

		messageHandler.HandleRequest(ctx, packet)
		*localConcurrency++
	case protocol.MESSAGE_RESPONSE:
		if packet.Err == ErrPacketDropped {
			return nil
		}

		messageHandler.HandleResponse(ctx, packet)
		*handledResponseCount++
	case protocol.MESSAGE_HANGUP:
		return &Error{packet.hangup.ErrorCode, true}
	default:
		panic("unreachable code")
	}

	if packet.Err != nil {
		self.options.Logger().Error().Err(packet.Err).
			Str("transport_id", self.GetTransportID().String()).
			Msg("stream_system_error")
		return self.hangUp(ctx, ErrorSystem)
	}

	return nil
}

func (self *Stream) hangUp(ctx context.Context, errorCode ErrorCode) error {
	if atomic.CompareAndSwapInt32(&self.isHungUp, 0, 1) {
		self.pendingHangup <- &protocol.Hangup{
			ErrorCode: errorCode,
		}
	}

	<-ctx.Done()
	return ctx.Err()
}

func (self *Stream) sendPackets(ctx context.Context, messageEmitter MessageEmitter) error {
	errs := make(chan error, 2)
	ctx, cancel := context.WithCancel(ctx)
	pendingRequests := make(chan *pendingMessages, 1)
	pendingResponses := make(chan *pendingMessages, 1)

	go func() {
		errs <- self.getPendingRequests(ctx, pendingRequests)
		cancel()
	}()

	go func() {
		errs <- self.getPendingResponses(ctx, pendingResponses)
		cancel()
	}()

	for {
		pendingRequests2, pendingResponses2, pendingHangup, err := self.checkPendingMessages(
			errs,
			pendingRequests,
			pendingResponses,
			self.outgoingKeepaliveInterval,
		)

		if err != nil {
			cancel()
			<-errs
			return err
		}

		err = self.emitPackets(pendingRequests2, pendingResponses2, pendingHangup, messageEmitter)

		if err != nil {
			if pendingRequests2 != nil {
				self.dequeOfPendingRequests.DiscardNodesRemoval(&pendingRequests2.List, pendingRequests2.ListLength, false)
			}

			if pendingResponses2 != nil {
				self.dequeOfPendingResponses.DiscardNodesRemoval(&pendingResponses2.List, pendingResponses2.ListLength, false)
			}
		}

		if err2 := self.flush(ctx, self.outgoingKeepaliveInterval/2*3); err2 != nil {
			err = err2
		}

		if err != nil {
			cancel()
			<-errs
			<-errs
			return err
		}
	}
}

func (self *Stream) getPendingRequests(ctx context.Context, pendingRequests chan *pendingMessages) error {
	pendingRequestsA := new(pendingMessages)
	pendingRequestsB := new(pendingMessages)

	for {
		pendingRequestsA.List.Init()
		pendingRequestsA.ListLength, _ = self.dequeOfPendingRequests.RemoveNodes(ctx, true, &pendingRequestsA.List)

		select {
		case pendingRequests <- pendingRequestsA:
			pendingRequestsA, pendingRequestsB = pendingRequestsB, pendingRequestsA
		case <-ctx.Done():
			self.dequeOfPendingRequests.DiscardNodesRemoval(&pendingRequestsA.List, pendingRequestsA.ListLength, false)
			return ctx.Err()
		}
	}
}

func (self *Stream) getPendingResponses(ctx context.Context, pendingResponses chan *pendingMessages) error {
	pendingResponsesA := new(pendingMessages)
	pendingResponsesB := new(pendingMessages)

	for {
		pendingResponsesA.List.Init()
		pendingResponsesA.ListLength, _ = self.dequeOfPendingResponses.RemoveNodes(ctx, true, &pendingResponsesA.List)

		select {
		case pendingResponses <- pendingResponsesA:
			pendingResponsesA, pendingResponsesB = pendingResponsesB, pendingResponsesA
		case <-ctx.Done():
			self.dequeOfPendingResponses.DiscardNodesRemoval(&pendingResponsesA.List, pendingResponsesA.ListLength, false)
			return ctx.Err()
		}
	}
}

func (self *Stream) checkPendingMessages(
	errs chan error,
	pendingRequests chan *pendingMessages,
	pendingResponses chan *pendingMessages,
	timeout time.Duration,
) (*pendingMessages, *pendingMessages, *protocol.Hangup, error) {
	pendingRequests2 := (*pendingMessages)(nil)
	pendingResponses2 := (*pendingMessages)(nil)
	pendingHangup := (*protocol.Hangup)(nil)

	select {
	case err := <-errs:
		return nil, nil, nil, err
	case pendingRequests2 = <-pendingRequests:
	case pendingResponses2 = <-pendingResponses:
	case pendingHangup = <-self.pendingHangup:
	case <-time.After(timeout):
	}

	if pendingRequests2 == nil {
		select {
		case pendingRequests2 = <-pendingRequests:
		default:
		}
	}

	if pendingResponses2 == nil {
		select {
		case pendingResponses2 = <-pendingResponses:
		default:
		}
	}

	if pendingHangup == nil {
		select {
		case pendingHangup = <-self.pendingHangup:
		default:
		}
	}

	return pendingRequests2, pendingResponses2, pendingHangup, nil
}

func (self *Stream) emitPackets(
	pendingRequests *pendingMessages,
	pendingResponses *pendingMessages,
	pendingHangup *protocol.Hangup,
	messageEmitter MessageEmitter,
) error {
	var packet Packet
	emittedPacketCount := 0
	err := error(nil)

	if pendingRequests != nil {
		now := time.Now().UnixNano()
		getListNode := pendingRequests.List.GetNodesSafely()
		emittedRequestCount := 0
		droppedRequestCount := 0

		for listNode := getListNode(); listNode != nil; listNode = getListNode() {
			pendingRequest := (*PendingRequest)(listNode.GetContainer(unsafe.Offsetof(PendingRequest{}.ListNode)))
			packet.MessageType = protocol.MESSAGE_REQUEST
			packet.RequestHeader = pendingRequest.Header
			packet.Message = pendingRequest.Payload

			if deadline := pendingRequest.Header.Deadline; deadline != 0 && deadline <= now {
				packet.Err = ErrRequestExpired
				messageEmitter.PostEmitRequest(&packet)
			} else {
				self.write(&packet, messageEmitter)
			}

			if packet.Err != nil && packet.Err != ErrPacketDropped {
				self.options.Logger().Error().Err(packet.Err).
					Str("transport_id", self.GetTransportID().String()).
					Msg("stream_system_error")
				err = packet.Err
				continue
			}

			listNode.Remove()
			listNode.Reset()
			pendingRequestPool.Put(pendingRequest)
			pendingRequests.ListLength--

			if packet.Err == nil {
				emittedRequestCount++
			} else {
				droppedRequestCount++
			}
		}

		self.dequeOfPendingRequests.CommitNodesRemoval(droppedRequestCount)
		emittedPacketCount += emittedRequestCount
	}

	if pendingResponses != nil {
		getListNode := pendingResponses.List.GetNodesSafely()
		emittedResponseCount := 0
		droppedResponseCount := 0

		for listNode := getListNode(); listNode != nil; listNode = getListNode() {
			pendingResponse := (*PendingResponse)(listNode.GetContainer(unsafe.Offsetof(PendingResponse{}.ListNode)))
			packet.MessageType = protocol.MESSAGE_RESPONSE
			packet.ResponseHeader = pendingResponse.Header
			packet.Message = pendingResponse.Payload
			self.write(&packet, messageEmitter)

			if packet.Err != nil && packet.Err != ErrPacketDropped {
				self.options.Logger().Error().Err(packet.Err).
					Str("transport_id", self.GetTransportID().String()).
					Msg("stream_system_error")
				err = packet.Err
				continue
			}

			listNode.Remove()
			listNode.Reset()
			pendingResponsePool.Put(pendingResponse)
			pendingResponses.ListLength--

			if packet.Err == nil {
				emittedResponseCount++
			} else {
				droppedResponseCount++
			}
		}

		self.dequeOfPendingResponses.CommitNodesRemoval(emittedResponseCount + droppedResponseCount)
		atomic.AddInt32(&self.localConcurrency, -int32(emittedResponseCount))
		emittedPacketCount += emittedResponseCount
	}

	if pendingHangup != nil {
		err = &Error{pendingHangup.ErrorCode, false}
	}

	if err == nil && emittedPacketCount == 0 {
		packet.MessageType = protocol.MESSAGE_KEEPALIVE
		self.write(&packet, nil)
		err = packet.Err
	}

	if err != nil {
		packet.MessageType = protocol.MESSAGE_HANGUP

		if error_, ok := err.(*Error); ok && !error_.IsFromWire {
			packet.hangup.ErrorCode = error_.Code
		} else {
			if err == transport.ErrPacketTooLarge {
				packet.hangup.ErrorCode = ErrorOutgoingPacketTooLarge
			} else {
				packet.hangup.ErrorCode = ErrorSystem
			}

			err = &Error{packet.hangup.ErrorCode, false}
		}

		self.write(&packet, nil)
	}

	return err
}

func (self *Stream) write(packet *Packet, messageEmitter MessageEmitter) {
	transportPacket := transport.Packet{
		Header: protocol.PacketHeader{
			MessageType: packet.MessageType,
		},
	}

	switch packet.MessageType {
	case protocol.MESSAGE_KEEPALIVE:
		packet.Message = nil
		packet.Err = nil
		messageEmitter.EmitKeepalive(packet)

		if packet.Err == nil {
			transportPacket.PayloadSize = packet.Message.Size()

			packet.Err = self.transport.Write(&transportPacket, func(buffer []byte) error {
				_, err := packet.Message.MarshalTo(buffer)
				return err
			})
		}
	case protocol.MESSAGE_REQUEST:
		requestHeaderSize := packet.RequestHeader.Size()
		transportPacket.PayloadSize = 4 + requestHeaderSize + packet.Message.Size()

		callback := func(buffer []byte) error {
			binary.BigEndian.PutUint32(buffer, uint32(requestHeaderSize))
			packet.RequestHeader.MarshalTo(buffer[4:])
			_, err := packet.Message.MarshalTo(buffer[4+requestHeaderSize:])
			return err
		}

		packet.Err = self.transport.Write(&transportPacket, callback)
		messageEmitter.PostEmitRequest(packet)
	case protocol.MESSAGE_RESPONSE:
		responseHeaderSize := packet.ResponseHeader.Size()
		transportPacket.PayloadSize = 4 + responseHeaderSize + packet.Message.Size()

		callback := func(buffer []byte) error {
			binary.BigEndian.PutUint32(buffer, uint32(responseHeaderSize))
			packet.ResponseHeader.MarshalTo(buffer[4:])
			_, err := packet.Message.MarshalTo(buffer[4+responseHeaderSize:])
			return err
		}

		packet.Err = self.transport.Write(&transportPacket, callback)
		messageEmitter.PostEmitResponse(packet)
	case protocol.MESSAGE_HANGUP:
		transportPacket.PayloadSize = packet.hangup.Size()

		self.transport.Write(&transportPacket, func(buffer []byte) error {
			packet.hangup.MarshalTo(buffer)
			return nil
		})
	default:
		panic("unreachable code")
	}
}

func (self *Stream) flush(ctx context.Context, timeout time.Duration) error {
	return self.transport.Flush(ctx, timeout)
}

type MessageProcessor interface {
	MessageFactory
	MessageHandler
	MessageEmitter
}

type MessageFactory interface {
	// Input:
	//   packet.KeepaliveHeader
	// Output:
	//   packet.Message
	//   packet.Err
	NewKeepalive(packet *Packet)

	// Input:
	//   packet.RequestHeader
	// Output:
	//   packet.Message
	//   packet.Err
	NewRequest(packet *Packet)

	// Input:
	//   packet.ResponseHeader
	// Output:
	//   packet.Message
	//   packet.Err
	NewResponse(packet *Packet)
}

type MessageHandler interface {
	// Input:
	//   packet.KeepaliveHeader
	//   packet.Message
	//   packet.Err
	// Output:
	//   packet.Err
	HandleKeepalive(ctx context.Context, packet *Packet)

	// Input:
	//   packet.RequestHeader
	//   packet.Message
	//   packet.Err
	// Output:
	//   packet.Err
	HandleRequest(ctx context.Context, packet *Packet)

	// Input:
	//   packet.ResponseHeader
	//   packet.Message
	//   packet.Err
	// Output:
	//   packet.Err
	HandleResponse(ctx context.Context, packet *Packet)
}

type MessageEmitter interface {
	// Output:
	//   packet.Message
	//   packet.Err
	EmitKeepalive(packet *Packet)

	// Input:
	//   packet.RequestHeader
	//   packet.Message
	//   packet.Err
	// Output:
	//   packet.RequestHeader
	//   packet.Message
	//   packet.Err
	PostEmitRequest(packet *Packet)

	// Input:
	//   packet.ResponseHeader
	//   packet.Message
	//   packet.Err
	// Output:
	//   packet.ResponseHeader
	//   packet.Message
	//   packet.Err
	PostEmitResponse(packet *Packet)
}

type Packet struct {
	MessageType    protocol.MessageType
	RequestHeader  protocol.RequestHeader
	ResponseHeader protocol.ResponseHeader
	Message        Message
	Err            error

	hangup protocol.Hangup
}

type PendingRequest struct {
	ListNode list.ListNode
	Header   protocol.RequestHeader
	Payload  Message
}

type PendingResponse struct {
	ListNode list.ListNode
	Header   protocol.ResponseHeader
	Payload  Message
}

var ErrConcurrencyOverflow = errors.New("pbrpc/stream: concurrency overflow")
var ErrClosed = errors.New("pbrpc/stream: closed")
var ErrPacketDropped = errors.New("pbrpc/stream: packet dropped")
var ErrRequestExpired = errors.New("pbrpc/stream: request expired")

func PutPooledPendingRequests(listOfPendingRequests *list.List) {
	getListNode := listOfPendingRequests.GetNodesSafely()

	for listNode := getListNode(); listNode != nil; listNode = getListNode() {
		pendingRequest := (*PendingRequest)(listNode.GetContainer(unsafe.Offsetof(PendingRequest{}.ListNode)))
		listNode.Remove()
		listNode.Reset()
		pendingRequestPool.Put(pendingRequest)
	}
}

func PutPooledPendingResponses(listOfPendingResponses *list.List) {
	getListNode := listOfPendingResponses.GetNodesSafely()

	for listNode := getListNode(); listNode != nil; listNode = getListNode() {
		pendingResponse := (*PendingResponse)(listNode.GetContainer(unsafe.Offsetof(PendingResponse{}.ListNode)))
		listNode.Remove()
		listNode.Reset()
		pendingResponsePool.Put(pendingResponse)
	}
}

type stream struct {
	options                   *Options
	transport                 transport.Transport
	incomingKeepaliveInterval time.Duration
	outgoingKeepaliveInterval time.Duration
	localConcurrencyLimit     int
	remoteConcurrencyLimit    int
}

func (self *stream) GetTransportID() uuid.UUID {
	return self.transport.GetID()
}

type pendingMessages struct {
	List       list.List
	ListLength int
}

var pendingRequestPool = sync.Pool{New: func() interface{} { return new(PendingRequest) }}
var pendingResponsePool = sync.Pool{New: func() interface{} { return new(PendingResponse) }}
