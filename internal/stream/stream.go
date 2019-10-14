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
	"github.com/let-z-go/toolkit/timerpool"
	"github.com/let-z-go/toolkit/uuid"

	"github.com/let-z-go/gogorpc/internal/protocol"
	"github.com/let-z-go/gogorpc/internal/transport"
)

type Stream struct {
	stream

	incomingConcurrency     int32
	deques                  [2]deque.Deque
	dequeOfPendingRequests  *deque.Deque
	dequeOfPendingResponses *deque.Deque
	isHungUp_               int32
	pendingHangup           chan *Hangup
}

func (self *Stream) Init(
	isServerSide bool,
	options *Options,
	transportID uuid.UUID,
	dequeOfPendingRequests *deque.Deque,
	dequeOfPendingResponses *deque.Deque,
) *Stream {
	self.isServerSide = isServerSide
	self.options = options.Normalize()
	self.transport.Init(self.options.Transport, transportID)

	if dequeOfPendingRequests == nil {
		dequeOfPendingRequests = self.deques[0].Init(0)
	}

	if dequeOfPendingResponses == nil {
		dequeOfPendingResponses = self.deques[1].Init(0)
	}

	self.dequeOfPendingRequests = dequeOfPendingRequests
	self.dequeOfPendingResponses = dequeOfPendingResponses
	self.pendingHangup = make(chan *Hangup, 1)
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

func (self *Stream) Establish(ctx context.Context, connection net.Conn, handshaker Handshaker) (bool, error) {
	transportHandshaker_ := transportHandshaker{
		Inner: handshaker,

		stream: &self.stream,
	}

	var ok bool
	var err error

	if self.isServerSide {
		ok, err = self.transport.PostAccept(ctx, connection, &transportHandshaker_)
	} else {
		ok, err = self.transport.PostConnect(ctx, connection, &transportHandshaker_)
	}

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
		errs <- err

		if _, ok := err.(*Hangup); ok {
			timer := timerpool.GetTimer(self.options.ActiveHangupTimeout)

			select {
			case <-ctx.Done():
				timerpool.StopAndPutTimer(timer)
				errs <- ctx.Err()
				return
			case <-timer.C: // wait for receivePackets returning EOF
				timerpool.PutTimer(timer)
			}
		}

		cancel()
	}()

	errs <- self.receivePackets(ctx, messageProcessor)
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
	self.hangUp(HangupAborted, extraData)
}

func (self *Stream) adjust() error {
	{
		n := self.outgoingConcurrencyLimit - self.dequeOfPendingRequests.MaxLength()

		if n < 0 {
			return ErrConcurrencyOverflow
		}

		self.dequeOfPendingRequests.CommitNodesRemoval(n) // dequeOfPendingRequests.maxLength += n
	}

	{
		n := self.incomingConcurrencyLimit - self.dequeOfPendingResponses.MaxLength()

		if n < 0 {
			return ErrConcurrencyOverflow
		}

		self.dequeOfPendingResponses.CommitNodesRemoval(n) // dequeOfPendingResponses.maxLength += n
	}

	return nil
}

func (self *Stream) receivePackets(ctx context.Context, messageProcessor MessageProcessor) error {
	packet := Packet{direction: Incoming}

	for {
		if err := self.peek(ctx, self.incomingKeepaliveInterval*4/3, messageProcessor, &packet); err != nil {
			return err
		}

		oldIncomingConcurrency := int(atomic.LoadInt32(&self.incomingConcurrency))
		newIncomingConcurrency := oldIncomingConcurrency
		handledResponseCount := 0

		if err := self.handlePacket(
			ctx,
			&packet,
			messageProcessor,
			&newIncomingConcurrency,
			&handledResponseCount,
		); err != nil {
			return err
		}

		for {
			ok, err := self.peekNext(messageProcessor, &packet)

			if err != nil {
				return err
			}

			if !ok {
				break
			}

			if err := self.handlePacket(
				ctx,
				&packet,
				messageProcessor,
				&newIncomingConcurrency,
				&handledResponseCount,
			); err != nil {
				return err
			}
		}

		atomic.AddInt32(&self.incomingConcurrency, int32(newIncomingConcurrency-oldIncomingConcurrency))
		self.dequeOfPendingRequests.CommitNodesRemoval(handledResponseCount)
	}
}

func (self *Stream) peek(ctx context.Context, timeout time.Duration, messageFactory MessageFactory, packet *Packet) error {
	var transportPacket transport.Packet

	if err := self.transport.Peek(ctx, timeout, &transportPacket); err != nil {
		return err
	}

	self.loadPacket(packet, &transportPacket, messageFactory)
	return nil
}

func (self *Stream) peekNext(messageFactory MessageFactory, packet *Packet) (bool, error) {
	var transportPacket transport.Packet
	ok, err := self.transport.PeekNext(&transportPacket)

	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	self.loadPacket(packet, &transportPacket, messageFactory)
	return true, nil
}

func (self *Stream) loadPacket(packet *Packet, transportPacket *transport.Packet, messageFactory MessageFactory) {
	packet.messageType = transportPacket.Header.MessageType

	switch packet.messageType {
	case MessageKeepalive:
		packet.Message = nil
		packet.Err = nil
		messageFactory.NewKeepalive(packet)

		if packet.Err == nil {
			packet.Err = packet.Message.Unmarshal(transportPacket.Payload)
		}
	case MessageRequest:
		if self.isHungUp() {
			packet.Err = ErrPacketDropped
			return
		}

		rawPacket := transportPacket.Payload
		packetSize := len(rawPacket)

		if packetSize < 4 {
			packet.Err = errBadPacket
			return
		}

		requestHeaderSize := int(int32(binary.BigEndian.Uint32(rawPacket)))
		requestOffset := 4 + requestHeaderSize

		if requestOffset < 4 || requestOffset > packetSize {
			packet.Err = errBadPacket
			return
		}

		requestHeader := &packet.RequestHeader
		requestHeader.Reset()

		if requestHeader.Unmarshal(rawPacket[4:requestOffset]) != nil {
			packet.Err = errBadPacket
			return
		}

		packet.Message = nil
		packet.Err = nil
		messageFactory.NewRequest(packet)

		if packet.Err == nil {
			packet.Err = packet.Message.Unmarshal(rawPacket[requestOffset:])
		}
	case MessageResponse:
		rawPacket := transportPacket.Payload
		packetSize := len(rawPacket)

		if packetSize < 4 {
			packet.Err = errBadPacket
			return
		}

		responseHeaderSize := int(int32(binary.BigEndian.Uint32(rawPacket)))
		responseOffset := 4 + responseHeaderSize

		if responseOffset < 4 || responseOffset > packetSize {
			packet.Err = errBadPacket
			return
		}

		responseHeader := &packet.ResponseHeader
		responseHeader.Reset()

		if responseHeader.Unmarshal(rawPacket[4:responseOffset]) != nil {
			packet.Err = errBadPacket
			return
		}

		packet.Message = nil
		packet.Err = nil
		messageFactory.NewResponse(packet)

		if packet.Err == nil {
			packet.Err = packet.Message.Unmarshal(rawPacket[responseOffset:])
		}
	case MessageHangup:
		hangup := &packet.Hangup
		hangup.Reset()

		if hangup.Unmarshal(transportPacket.Payload) != nil {
			packet.Err = errBadPacket
			return
		}

		packet.Err = nil
	default:
		packet.Err = errBadPacket
	}
}

func (self *Stream) handlePacket(
	ctx context.Context,
	packet *Packet,
	messageHandler MessageHandler,
	incomingConcurrency *int,
	handledResponseCount *int,
) error {
	if packet.Err == errBadPacket {
		self.hangUp(HangupBadIncomingPacket, nil)
		return nil
	}

	if packet.Err == nil {
		self.filterPacket(packet)
	}

	if packet.Err == ErrPacketDropped {
		return nil
	}

	switch packet.messageType {
	case MessageKeepalive:
		messageHandler.HandleKeepalive(ctx, packet)

		if packet.Err == nil {
			self.transport.ShrinkInputBuffer()
		}
	case MessageRequest:
		if *incomingConcurrency == self.incomingConcurrencyLimit {
			self.hangUp(HangupTooManyIncomingRequests, nil)
			return nil
		}

		messageHandler.HandleRequest(ctx, packet)
		*incomingConcurrency++
	case MessageResponse:
		messageHandler.HandleResponse(ctx, packet)
		*handledResponseCount++
	case MessageHangup:
		if packet.Err == nil {
			self.options.Logger.Info().Err(packet.Err).
				Str("transport_id", self.TransportID().String()).
				Msg("stream_passive_hangup")
			hangup := &packet.Hangup

			return &Hangup{
				IsPassive: true,
				Code:      hangup.Code,
				ExtraData: hangup.ExtraData,
			}
		}
	default:
		panic("unreachable code")
	}

	if packet.Err != nil {
		self.options.Logger.Error().Err(packet.Err).
			Str("transport_id", self.TransportID().String()).
			Msg("stream_system_error")
		self.hangUp(HangupSystem, nil)
	}

	return nil
}

func (self *Stream) hangUp(hangupCode HangupCode, extraData ExtraData) {
	if atomic.CompareAndSwapInt32(&self.isHungUp_, 0, 1) {
		self.pendingHangup <- &Hangup{
			IsPassive: false,
			Code:      hangupCode,
			ExtraData: extraData,
		}
	}
}

func (self *Stream) sendPackets(ctx context.Context, messageProcessor MessageProcessor) error {
	errs := make(chan error, 2)
	ctx, cancel := context.WithCancel(ctx)
	pendingRequests := self.makePendingRequests(errs, ctx, cancel)
	pendingResponses := self.makePendingResponses(errs, ctx, cancel)
	packet := Packet{direction: Outgoing}

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

		err = self.emitPackets(pendingRequests2, pendingResponses2, pendingHangup, &packet, messageProcessor)

		if err != nil {
			if pendingRequests2 != nil {
				self.dequeOfPendingRequests.DiscardNodesRemoval(&pendingRequests2.List, pendingRequests2.ListLength, false)
			}

			if pendingResponses2 != nil {
				self.dequeOfPendingResponses.DiscardNodesRemoval(&pendingResponses2.List, pendingResponses2.ListLength, false)
			}
		}

		if err2 := self.flush(ctx, self.outgoingKeepaliveInterval*4/3); err2 != nil {
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
		case <-ctx.Done():
			self.dequeOfPendingRequests.DiscardNodesRemoval(&pendingRequestsA.List, pendingRequestsA.ListLength, false)
			return ctx.Err()
		case pendingRequests <- pendingRequestsA:
			pendingRequestsA, pendingRequestsB = pendingRequestsB, pendingRequestsA
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
		case <-ctx.Done():
			self.dequeOfPendingResponses.DiscardNodesRemoval(&pendingResponsesA.List, pendingResponsesA.ListLength, false)
			return ctx.Err()
		case pendingResponses <- pendingResponsesA:
			pendingResponsesA, pendingResponsesB = pendingResponsesB, pendingResponsesA
		}
	}
}

func (self *Stream) checkPendingMessages(
	errs chan error,
	pendingRequests chan *pendingMessages,
	pendingResponses chan *pendingMessages,
	timeout time.Duration,
) (*pendingMessages, *pendingMessages, *Hangup, error) {
	var pendingRequests2 *pendingMessages
	var pendingResponses2 *pendingMessages
	var pendingHangup *Hangup
	n := 0

	select {
	case pendingRequests2 = <-pendingRequests:
		n++
	default:
	}

	select {
	case pendingResponses2 = <-pendingResponses:
		n++
	default:
	}

	select {
	case pendingHangup = <-self.pendingHangup:
		n++
	default:
	}

	if n == 0 {
		timer := timerpool.GetTimer(timeout)

		select {
		case err := <-errs:
			timerpool.StopAndPutTimer(timer)
			return nil, nil, nil, err
		case pendingRequests2 = <-pendingRequests:
			timerpool.StopAndPutTimer(timer)
		case pendingResponses2 = <-pendingResponses:
			timerpool.StopAndPutTimer(timer)
		case pendingHangup = <-self.pendingHangup:
			timerpool.StopAndPutTimer(timer)
		case <-timer.C:
			timerpool.PutTimer(timer)
		}
	}

	return pendingRequests2, pendingResponses2, pendingHangup, nil
}

func (self *Stream) emitPackets(
	pendingRequests *pendingMessages,
	pendingResponses *pendingMessages,
	pendingHangup *Hangup,
	packet *Packet,
	messageEmitter MessageEmitter,
) error {
	err := error(nil)
	emittedPacketCount := 0

	if pendingRequests != nil {
		now := time.Now().UnixNano()
		getListNode := pendingRequests.List.GetNodesSafely()
		emittedRequestCount := 0
		droppedRequestCount := 0

		for listNode := getListNode(); listNode != nil; listNode = getListNode() {
			pendingRequest := (*PendingRequest)(listNode.GetContainer(unsafe.Offsetof(PendingRequest{}.ListNode)))
			packet.messageType = MessageRequest
			packet.RequestHeader = pendingRequest.Header
			packet.Message = pendingRequest.Payload
			var ok bool

			if deadline := pendingRequest.Header.Deadline; deadline != 0 && deadline <= now {
				packet.Err = ErrRequestExpired
				messageEmitter.PostEmitRequest(packet)
				ok, err = packet.Err == nil, packet.Err

				if err == ErrPacketDropped {
					err = nil
				}
			} else {
				ok, err = self.write(packet, messageEmitter)
			}

			if err != nil {
				self.options.Logger.Error().Err(err).
					Str("transport_id", self.TransportID().String()).
					Msg("stream_system_error")
				continue
			}

			listNode.Remove()
			listNode.Reset()
			pendingRequestPool.Put(pendingRequest)
			pendingRequests.ListLength--

			if ok {
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
			packet.messageType = MessageResponse
			packet.ResponseHeader = pendingResponse.Header
			packet.Message = pendingResponse.Payload
			var ok bool
			ok, err = self.write(packet, messageEmitter)

			if err != nil {
				self.options.Logger.Error().Err(err).
					Str("transport_id", self.TransportID().String()).
					Msg("stream_system_error")
				continue
			}

			listNode.Remove()
			listNode.Reset()
			pendingResponsePool.Put(pendingResponse)
			pendingResponses.ListLength--

			if ok {
				emittedResponseCount++
			} else {
				droppedResponseCount++
			}
		}

		self.dequeOfPendingResponses.CommitNodesRemoval(emittedResponseCount + droppedResponseCount)
		atomic.AddInt32(&self.incomingConcurrency, -int32(emittedResponseCount))
		emittedPacketCount += emittedResponseCount
	}

	if pendingHangup != nil {
		self.options.Logger.Info().
			Str("transport_id", self.TransportID().String()).
			Msg("stream_active_hangup")
		packet.messageType = MessageHangup
		packet.Hangup.Code = pendingHangup.Code
		packet.Hangup.ExtraData = pendingHangup.ExtraData
		_, err = self.write(packet, messageEmitter)

		if err != nil {
			return err
		}

		return pendingHangup
	}

	if err == nil && emittedPacketCount == 0 {
		packet.messageType = MessageKeepalive
		_, err = self.write(packet, messageEmitter)
	}

	if err != nil {
		if err == transport.ErrPacketTooLarge {
			self.hangUp(HangupOutgoingPacketTooLarge, nil)
		} else {
			self.hangUp(HangupSystem, nil)
		}
	}

	return nil
}

func (self *Stream) write(packet *Packet, messageEmitter MessageEmitter) (bool, error) {
	transportPacket := transport.Packet{
		Header: protocol.PacketHeader{
			MessageType: packet.messageType,
		},
	}

	packet.Err = nil

	switch packet.messageType {
	case MessageKeepalive:
		packet.Message = nil
		messageEmitter.EmitKeepalive(packet)

		if packet.Err == nil {
			self.filterPacket(packet)

			if packet.Err == nil {
				transportPacket.PayloadSize = packet.Message.Size()

				packet.Err = self.transport.Write(&transportPacket, func(buffer []byte) error {
					_, err := packet.Message.MarshalTo(buffer)
					return err
				})

				if packet.Err == nil {
					self.transport.ShrinkOutputBuffer()
				}
			}
		}
	case MessageRequest:
		self.filterPacket(packet)

		if packet.Err == nil {
			requestHeader := &packet.RequestHeader
			requestHeaderSize := requestHeader.Size()
			transportPacket.PayloadSize = 4 + requestHeaderSize + packet.Message.Size()

			packet.Err = self.transport.Write(&transportPacket, func(buffer []byte) error {
				binary.BigEndian.PutUint32(buffer, uint32(requestHeaderSize))
				requestHeader.MarshalTo(buffer[4:])
				_, err := packet.Message.MarshalTo(buffer[4+requestHeaderSize:])
				return err
			})
		}

		messageEmitter.PostEmitRequest(packet)
	case MessageResponse:
		self.filterPacket(packet)

		if packet.Err == nil {
			responseHeader := &packet.ResponseHeader
			responseHeaderSize := responseHeader.Size()
			transportPacket.PayloadSize = 4 + responseHeaderSize + packet.Message.Size()

			packet.Err = self.transport.Write(&transportPacket, func(buffer []byte) error {
				binary.BigEndian.PutUint32(buffer, uint32(responseHeaderSize))
				responseHeader.MarshalTo(buffer[4:])
				_, err := packet.Message.MarshalTo(buffer[4+responseHeaderSize:])
				return err
			})
		}

		messageEmitter.PostEmitResponse(packet)
	case MessageHangup:
		self.filterPacket(packet)

		if packet.Err == nil {
			hangup := &packet.Hangup
			transportPacket.PayloadSize = hangup.Size()

			packet.Err = self.transport.Write(&transportPacket, func(buffer []byte) error {
				hangup.MarshalTo(buffer)
				return nil
			})
		}
	default:
		panic("unreachable code")
	}

	if err := packet.Err; err != nil {
		if err == ErrPacketDropped {
			err = nil
		}

		return false, err
	}

	return true, nil
}

func (self *Stream) flush(ctx context.Context, timeout time.Duration) error {
	return self.transport.Flush(ctx, timeout)
}

func (self *Stream) filterPacket(packet *Packet) {
	packetFilters := self.options.DoGetPacketFilters(packet.direction, packet.messageType)

	for _, packetFilter := range packetFilters {
		if !packetFilter(packet) {
			break
		}
	}
}

func (self *Stream) isHungUp() bool {
	return atomic.LoadInt32(&self.isHungUp_) == 1
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

var (
	ErrConcurrencyOverflow = errors.New("gogorpc/stream: concurrency overflow")
	ErrClosed              = errors.New("gogorpc/stream: closed")
	ErrRequestExpired      = errors.New("gogorpc/stream: request expired")
)

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
	isServerSide              bool
	options                   *Options
	transport                 transport.Transport
	incomingKeepaliveInterval time.Duration
	outgoingKeepaliveInterval time.Duration
	incomingConcurrencyLimit  int
	outgoingConcurrencyLimit  int
}

func (self *stream) TransportID() uuid.UUID {
	return self.transport.ID()
}

func (self *stream) IsServerSide() bool {
	return self.isServerSide
}

type pendingMessages struct {
	List       list.List
	ListLength int
}

var errBadPacket = errors.New("gogorpc/stream: bad packet")

var (
	pendingRequestPool  = sync.Pool{New: func() interface{} { return new(PendingRequest) }}
	pendingResponsePool = sync.Pool{New: func() interface{} { return new(PendingResponse) }}
)
