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
		err := self.sendPackets(ctx, messageProcessor)

		if _, ok := err.(*HangupError); ok {
			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			case <-time.After(self.options.ActiveHangupTimeout):
				// wait for receivePackets returning EOF
			}
		}

		errs <- err
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

func (self *Stream) Abort(extraData ExtraData) {
	self.hangUp(HangupErrorAborted, extraData)
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

	self.parseTransportPacket(ctx, &transportPacket, messageFactory, packet)
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

	self.parseTransportPacket(ctx, &transportPacket, messageFactory, packet)
	return true, nil
}

func (self *Stream) parseTransportPacket(ctx context.Context, transportPacket *transport.Packet, messageFactory MessageFactory, packet *Packet) {
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
			packet.Err = errBadPacket
			return
		}

		requestHeaderSize := int(int32(binary.BigEndian.Uint32(rawPacket)))
		requestOffset := 4 + requestHeaderSize

		if requestHeaderSize < 0 || requestOffset > packetSize {
			packet.Err = errBadPacket
			return
		}

		packet.RequestHeader.Reset()

		if packet.RequestHeader.Unmarshal(rawPacket[4:requestOffset]) != nil {
			packet.Err = errBadPacket
			return
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
			packet.Err = errBadPacket
			return
		}

		responseHeaderSize := int(int32(binary.BigEndian.Uint32(rawPacket)))
		responseOffset := 4 + responseHeaderSize

		if responseHeaderSize < 0 || responseOffset > packetSize {
			packet.Err = errBadPacket
			return
		}

		packet.ResponseHeader.Reset()

		if packet.ResponseHeader.Unmarshal(rawPacket[4:responseOffset]) != nil {
			packet.Err = errBadPacket
			return
		}

		packet.Message = nil
		packet.Err = nil
		messageFactory.NewResponse(packet)

		if packet.Err == nil {
			packet.Err = packet.Message.Unmarshal(rawPacket[responseOffset:])
		}
	case protocol.MESSAGE_HANGUP:
		packet.hangup.Reset()
		packet.Err = packet.hangup.Unmarshal(transportPacket.Payload)
	default:
		packet.Err = errBadPacket
	}
}

func (self *Stream) handlePacket(
	ctx context.Context,
	packet *Packet,
	messageHandler MessageHandler,
	localConcurrency *int,
	handledResponseCount *int,
) error {
	if packet.Err == errBadPacket {
		self.hangUp(HangupErrorBadIncomingPacket, nil)
		return nil
	}

	switch packet.MessageType {
	case protocol.MESSAGE_KEEPALIVE:
		messageHandler.HandleKeepalive(ctx, packet)
	case protocol.MESSAGE_REQUEST:
		if packet.Err == ErrPacketDropped {
			return nil
		}

		if *localConcurrency == self.localConcurrencyLimit {
			self.hangUp(HangupErrorTooManyIncomingRequests, nil)
			return nil
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
		self.options.Logger().Error().Err(packet.Err).
			Str("transport_id", self.GetTransportID().String()).
			Msg("stream_passive_hangup")
		return &HangupError{packet.hangup.ErrorCode, true, packet.hangup.ExtraData}
	default:
		panic("unreachable code")
	}

	if packet.Err != nil {
		self.options.Logger().Error().Err(packet.Err).
			Str("transport_id", self.GetTransportID().String()).
			Msg("stream_system_error")
		self.hangUp(HangupErrorSystem, nil)
	}

	return nil
}

func (self *Stream) hangUp(hangupErrorCode HangupErrorCode, extraData ExtraData) {
	if atomic.CompareAndSwapInt32(&self.isHungUp, 0, 1) {
		self.pendingHangup <- &protocol.Hangup{
			ErrorCode: hangupErrorCode,
			ExtraData: extraData,
		}
	}
}

func (self *Stream) sendPackets(ctx context.Context, messageEmitter MessageEmitter) error {
	errs := make(chan error, 2)
	ctx, cancel := context.WithCancel(ctx)
	pendingRequests := self.makePendingRequests(errs, ctx, cancel)
	pendingResponses := self.makePendingResponses(errs, ctx, cancel)

	for {
		pendingRequests2, pendingResponses2, pendingHangup, err := self.getPendingMessages(
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

func (self *Stream) makePendingRequests(errs chan error, ctx context.Context, cancel context.CancelFunc) chan *pendingMessages {
	pendingRequests := make(chan *pendingMessages)

	go func() {
		errs <- self.putPendingRequests(ctx, pendingRequests)
		cancel()
	}()

	return pendingRequests
}

func (self *Stream) putPendingRequests(ctx context.Context, pendingRequests chan *pendingMessages) error {
	pendingRequestsA := new(pendingMessages)
	pendingRequestsB := new(pendingMessages)

	for {
		pendingRequestsA.List.Init()
		var err error
		pendingRequestsA.ListLength, err = self.dequeOfPendingRequests.RemoveNodes(ctx, true, &pendingRequestsA.List)

		if err != nil {
			return err
		}

		select {
		case pendingRequests <- pendingRequestsA:
			pendingRequestsA, pendingRequestsB = pendingRequestsB, pendingRequestsA
		case <-ctx.Done():
			self.dequeOfPendingRequests.DiscardNodesRemoval(&pendingRequestsA.List, pendingRequestsA.ListLength, false)
			return ctx.Err()
		}
	}
}

func (self *Stream) makePendingResponses(errs chan error, ctx context.Context, cancel context.CancelFunc) chan *pendingMessages {
	pendingResponses := make(chan *pendingMessages)

	go func() {
		errs <- self.putPendingResponses(ctx, pendingResponses)
		cancel()
	}()

	return pendingResponses
}

func (self *Stream) putPendingResponses(ctx context.Context, pendingResponses chan *pendingMessages) error {
	pendingResponsesA := new(pendingMessages)
	pendingResponsesB := new(pendingMessages)

	for {
		pendingResponsesA.List.Init()
		var err error
		pendingResponsesA.ListLength, err = self.dequeOfPendingResponses.RemoveNodes(ctx, true, &pendingResponsesA.List)

		if err != nil {
			return err
		}

		select {
		case pendingResponses <- pendingResponsesA:
			pendingResponsesA, pendingResponsesB = pendingResponsesB, pendingResponsesA
		case <-ctx.Done():
			self.dequeOfPendingResponses.DiscardNodesRemoval(&pendingResponsesA.List, pendingResponsesA.ListLength, false)
			return ctx.Err()
		}
	}
}

func (self *Stream) getPendingMessages(
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
	now := time.Now().UnixNano()
	packetErr := error(nil)
	emittedPacketCount := 0

	if pendingRequests != nil {
		getListNode := pendingRequests.List.GetNodesSafely()
		emittedRequestCount := 0
		droppedRequestCount := 0

		for listNode := getListNode(); listNode != nil; listNode = getListNode() {
			pendingRequest := (*PendingRequest)(listNode.GetContainer(unsafe.Offsetof(PendingRequest{}.ListNode)))
			packet.MessageType = protocol.MESSAGE_REQUEST
			packet.RequestHeader = pendingRequest.Header
			packet.Message = pendingRequest.Payload
			self.write(&packet, now, messageEmitter)

			if packet.Err != nil && packet.Err != ErrPacketDropped {
				self.options.Logger().Error().Err(packet.Err).
					Str("transport_id", self.GetTransportID().String()).
					Msg("stream_system_error")
				packetErr = packet.Err
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
			self.write(&packet, now, messageEmitter)

			if packet.Err != nil && packet.Err != ErrPacketDropped {
				self.options.Logger().Error().Err(packet.Err).
					Str("transport_id", self.GetTransportID().String()).
					Msg("stream_system_error")
				packetErr = packet.Err
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
		self.options.Logger().Error().Err(packet.Err).
			Str("transport_id", self.GetTransportID().String()).
			Msg("stream_active_hangup")
		packet.MessageType = protocol.MESSAGE_HANGUP
		packet.hangup = *pendingHangup
		self.write(&packet, now, messageEmitter)
		return &HangupError{pendingHangup.ErrorCode, false, pendingHangup.ExtraData}
	}

	if packetErr == nil && emittedPacketCount == 0 {
		packet.MessageType = protocol.MESSAGE_KEEPALIVE
		self.write(&packet, now, messageEmitter)
		packetErr = packet.Err
	}

	if packetErr != nil {
		if packetErr == transport.ErrPacketTooLarge {
			self.hangUp(HangupErrorOutgoingPacketTooLarge, nil)
		} else {
			self.hangUp(HangupErrorSystem, nil)
		}
	}

	return nil
}

func (self *Stream) write(packet *Packet, now int64, messageEmitter MessageEmitter) {
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
		if packet.RequestHeader.Deadline != 0 && packet.RequestHeader.Deadline <= now {
			packet.Err = ErrRequestExpired
		} else {
			requestHeaderSize := packet.RequestHeader.Size()
			transportPacket.PayloadSize = 4 + requestHeaderSize + packet.Message.Size()

			callback := func(buffer []byte) error {
				binary.BigEndian.PutUint32(buffer, uint32(requestHeaderSize))
				packet.RequestHeader.MarshalTo(buffer[4:])
				_, err := packet.Message.MarshalTo(buffer[4+requestHeaderSize:])
				return err
			}

			packet.Err = self.transport.Write(&transportPacket, callback)
		}

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

		packet.Err = self.transport.Write(&transportPacket, func(buffer []byte) error {
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

var errBadPacket = errors.New("pbrpc/stream: bad packet")

var pendingRequestPool = sync.Pool{New: func() interface{} { return new(PendingRequest) }}
var pendingResponsePool = sync.Pool{New: func() interface{} { return new(PendingResponse) }}
