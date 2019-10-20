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

	"github.com/let-z-go/gogorpc/internal/proto"
	"github.com/let-z-go/gogorpc/internal/transport"
)

type Stream struct {
	isServerSide              bool
	options                   *Options
	userData                  interface{}
	transport                 transport.Transport
	deques                    [2]deque.Deque
	dequeOfPendingRequests    *deque.Deque
	dequeOfPendingResponses   *deque.Deque
	isHungUp_                 int32
	pendingHangup             chan *Hangup
	incomingKeepaliveInterval time.Duration
	outgoingKeepaliveInterval time.Duration
	incomingConcurrencyLimit  int
	outgoingConcurrencyLimit  int
	incomingConcurrency       int32
}

func (self *Stream) Init(
	isServerSide bool,
	options *Options,
	userData interface{},
	transportID uuid.UUID,
	dequeOfPendingRequests *deque.Deque,
	dequeOfPendingResponses *deque.Deque,
) *Stream {
	self.isServerSide = isServerSide
	self.options = options.Normalize()
	self.userData = userData
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
		Underlying: handshaker,

		stream: self,
	}

	if self.isServerSide {
		return self.transport.PostAccept(ctx, connection, &transportHandshaker_)
	} else {
		return self.transport.PostConnect(ctx, connection, &transportHandshaker_)
	}
}

func (self *Stream) Process(
	ctx context.Context,
	trafficCrypter transport.TrafficCrypter,
	messageProcessor MessageProcessor,
) error {
	self.transport.Prepare(trafficCrypter)

	if err := self.prepare(); err != nil {
		return err
	}

	errs := make(chan error, 2)
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		err := self.sendEvents(ctx, messageProcessor, trafficCrypter)
		errs <- err

		if _, ok := err.(*Hangup); ok {
			timer := timerpool.GetTimer(self.options.ActiveHangupTimeout)

			select {
			case <-ctx.Done():
				timerpool.StopAndPutTimer(timer)
				errs <- ctx.Err()
				return
			case <-timer.C: // wait for receiveEvents returning EOF
				timerpool.PutTimer(timer)
			}
		}

		cancel()
	}()

	errs <- self.receiveEvents(ctx, trafficCrypter, messageProcessor, messageProcessor)
	cancel()
	err := <-errs
	<-errs
	return err
}

func (self *Stream) SendRequest(ctx context.Context, requestHeader *proto.RequestHeader, request Message) error {
	pendingRequest := pendingRequestPool.Get().(*PendingRequest)
	pendingRequest.Header = *requestHeader
	pendingRequest.Underlying = request

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

func (self *Stream) SendResponse(responseHeader *proto.ResponseHeader, response Message) error {
	pendingResponse := pendingResponsePool.Get().(*PendingResponse)
	pendingResponse.Header = *responseHeader
	pendingResponse.Underlying = response

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

func (self *Stream) IsServerSide() bool {
	return self.isServerSide
}

func (self *Stream) UserData() interface{} {
	return self.userData
}

func (self *Stream) TransportID() uuid.UUID {
	return self.transport.ID()
}

func (self *Stream) prepare() error {
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

func (self *Stream) receiveEvents(
	ctx context.Context,
	trafficDecrypter transport.TrafficDecrypter,
	messageFactory MessageFactory,
	messageHandler MessageHandler,
) error {
	event := Event{
		stream:    self,
		direction: EventIncoming,
	}

	for {
		if err := self.peek(
			ctx,
			self.incomingKeepaliveInterval*4/3,
			trafficDecrypter,
			messageFactory,
			&event,
		); err != nil {
			return err
		}

		oldIncomingConcurrency := int(atomic.LoadInt32(&self.incomingConcurrency))
		newIncomingConcurrency := oldIncomingConcurrency
		handledResponseCount := 0

		if err := self.handleEvent(
			ctx,
			&event,
			messageHandler,
			&newIncomingConcurrency,
			&handledResponseCount,
		); err != nil {
			return err
		}

		for {
			ok, err := self.peekNext(messageFactory, &event)

			if err != nil {
				return err
			}

			if !ok {
				break
			}

			if err := self.handleEvent(
				ctx,
				&event,
				messageHandler,
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

func (self *Stream) peek(
	ctx context.Context,
	timeout time.Duration,
	trafficDecrypter transport.TrafficDecrypter,
	messageFactory MessageFactory,
	event *Event,
) error {
	var packet transport.Packet

	if err := self.transport.Peek(ctx, timeout, trafficDecrypter, &packet); err != nil {
		return err
	}

	self.loadEvent(event, &packet, messageFactory)
	return nil
}

func (self *Stream) peekNext(messageFactory MessageFactory, event *Event) (bool, error) {
	var packet transport.Packet
	ok, err := self.transport.PeekNext(&packet)

	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	self.loadEvent(event, &packet, messageFactory)
	return true, nil
}

func (self *Stream) loadEvent(event *Event, packet *transport.Packet, messageFactory MessageFactory) {
	event.type_ = packet.Header.EventType

	switch event.type_ {
	case EventKeepalive:
		event.Message = nil
		event.Err = nil
		messageFactory.NewKeepalive(event)

		if event.Err == nil {
			event.Err = event.Message.Unmarshal(packet.Payload)
		}
	case EventRequest:
		if self.isHungUp() {
			event.Err = ErrEventDropped
			return
		}

		rawEvent := packet.Payload
		rawEventSize := len(rawEvent)

		if rawEventSize < 4 {
			event.Err = errBadEvent
			return
		}

		requestHeaderSize := int(int32(binary.BigEndian.Uint32(rawEvent)))
		rawRequestOffset := 4 + requestHeaderSize

		if rawRequestOffset < 4 || rawRequestOffset > rawEventSize {
			event.Err = errBadEvent
			return
		}

		requestHeader := &event.RequestHeader
		requestHeader.Reset()

		if requestHeader.Unmarshal(rawEvent[4:rawRequestOffset]) != nil {
			event.Err = errBadEvent
			return
		}

		event.Message = nil
		event.Err = nil
		messageFactory.NewRequest(event)

		if event.Err == nil {
			event.Err = event.Message.Unmarshal(rawEvent[rawRequestOffset:])
		}
	case EventResponse:
		rawEvent := packet.Payload
		rawEventSize := len(rawEvent)

		if rawEventSize < 4 {
			event.Err = errBadEvent
			return
		}

		responseHeaderSize := int(int32(binary.BigEndian.Uint32(rawEvent)))
		rawResponseOffset := 4 + responseHeaderSize

		if rawResponseOffset < 4 || rawResponseOffset > rawEventSize {
			event.Err = errBadEvent
			return
		}

		responseHeader := &event.ResponseHeader
		responseHeader.Reset()

		if responseHeader.Unmarshal(rawEvent[4:rawResponseOffset]) != nil {
			event.Err = errBadEvent
			return
		}

		event.Message = nil
		event.Err = nil
		messageFactory.NewResponse(event)

		if event.Err == nil {
			event.Err = event.Message.Unmarshal(rawEvent[rawResponseOffset:])
		}
	case EventHangup:
		hangup := &event.Hangup
		hangup.Reset()

		if hangup.Unmarshal(packet.Payload) != nil {
			event.Err = errBadEvent
			return
		}

		event.Err = nil
	default:
		event.Err = errBadEvent
	}
}

func (self *Stream) handleEvent(
	ctx context.Context,
	event *Event,
	messageHandler MessageHandler,
	incomingConcurrency *int,
	handledResponseCount *int,
) error {
	if event.Err == errBadEvent {
		self.hangUp(HangupBadIncomingEvent, nil)
		return nil
	}

	if event.Err == nil {
		self.filterEvent(event)
	}

	if event.Err == ErrEventDropped {
		return nil
	}

	switch event.type_ {
	case EventKeepalive:
		messageHandler.HandleKeepalive(ctx, event)

		if event.Err == nil {
			self.transport.ShrinkInputBuffer()
		}
	case EventRequest:
		if *incomingConcurrency == self.incomingConcurrencyLimit {
			self.hangUp(HangupTooManyIncomingRequests, nil)
			return nil
		}

		messageHandler.HandleRequest(ctx, event)
		*incomingConcurrency++
	case EventResponse:
		messageHandler.HandleResponse(ctx, event)
		*handledResponseCount++
	case EventHangup:
		if event.Err == nil {
			self.options.Logger.Info().Err(event.Err).
				Str("transport_id", self.TransportID().String()).
				Msg("stream_passive_hangup")
			hangup := &event.Hangup

			return &Hangup{
				IsPassive: true,
				Code:      hangup.Code,
				ExtraData: hangup.ExtraData,
			}
		}
	default:
		panic("unreachable code")
	}

	if event.Err != nil {
		self.options.Logger.Error().Err(event.Err).
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

func (self *Stream) sendEvents(
	ctx context.Context,
	messageEmitter MessageEmitter,
	trafficEncrypter transport.TrafficEncrypter,
) error {
	errs := make(chan error, 2)
	ctx, cancel := context.WithCancel(ctx)
	pendingRequests := self.makePendingRequests(errs, ctx, cancel)
	pendingResponses := self.makePendingResponses(errs, ctx, cancel)

	event := Event{
		stream:    self,
		direction: EventOutgoing,
	}

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

		err = self.emitEvents(pendingRequests2, pendingResponses2, pendingHangup, &event, messageEmitter)

		if err != nil {
			if pendingRequests2 != nil {
				self.dequeOfPendingRequests.DiscardNodesRemoval(&pendingRequests2.List, pendingRequests2.ListLength, false)
			}

			if pendingResponses2 != nil {
				self.dequeOfPendingResponses.DiscardNodesRemoval(&pendingResponses2.List, pendingResponses2.ListLength, false)
			}
		}

		if err2 := self.flush(ctx, self.outgoingKeepaliveInterval*4/3, trafficEncrypter); err2 != nil {
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

func (self *Stream) emitEvents(
	pendingRequests *pendingMessages,
	pendingResponses *pendingMessages,
	pendingHangup *Hangup,
	event *Event,
	messageEmitter MessageEmitter,
) error {
	err := error(nil)
	emittedEventCount := 0

	if pendingRequests != nil {
		now := time.Now().UnixNano()
		getListNode := pendingRequests.List.GetNodesSafely()
		emittedRequestCount := 0
		droppedRequestCount := 0

		for listNode := getListNode(); listNode != nil; listNode = getListNode() {
			pendingRequest := (*PendingRequest)(listNode.GetContainer(unsafe.Offsetof(PendingRequest{}.ListNode)))
			event.type_ = EventRequest
			event.RequestHeader = pendingRequest.Header
			event.Message = pendingRequest.Underlying
			var ok bool

			if deadline := pendingRequest.Header.Deadline; deadline != 0 && deadline <= now {
				event.Err = ErrRequestExpired
				messageEmitter.PostEmitRequest(event)
				ok, err = event.Err == nil, event.Err

				if err == ErrEventDropped {
					err = nil
				}
			} else {
				ok, err = self.write(event, messageEmitter)
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
		emittedEventCount += emittedRequestCount
	}

	if pendingResponses != nil {
		getListNode := pendingResponses.List.GetNodesSafely()
		emittedResponseCount := 0
		droppedResponseCount := 0

		for listNode := getListNode(); listNode != nil; listNode = getListNode() {
			pendingResponse := (*PendingResponse)(listNode.GetContainer(unsafe.Offsetof(PendingResponse{}.ListNode)))
			event.type_ = EventResponse
			event.ResponseHeader = pendingResponse.Header
			event.Message = pendingResponse.Underlying
			var ok bool
			ok, err = self.write(event, messageEmitter)

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
		emittedEventCount += emittedResponseCount
	}

	if pendingHangup != nil {
		self.options.Logger.Info().
			Str("transport_id", self.TransportID().String()).
			Msg("stream_active_hangup")
		event.type_ = EventHangup
		event.Hangup.Code = pendingHangup.Code
		event.Hangup.ExtraData = pendingHangup.ExtraData
		_, err = self.write(event, messageEmitter)

		if err != nil {
			return err
		}

		return pendingHangup
	}

	if err == nil && emittedEventCount == 0 {
		event.type_ = EventKeepalive
		_, err = self.write(event, messageEmitter)
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

func (self *Stream) write(event *Event, messageEmitter MessageEmitter) (bool, error) {
	packet := transport.Packet{
		Header: proto.PacketHeader{
			EventType: event.type_,
		},
	}

	event.Err = nil

	switch event.type_ {
	case EventKeepalive:
		event.Message = nil
		messageEmitter.EmitKeepalive(event)

		if event.Err == nil {
			self.filterEvent(event)

			if event.Err == nil {
				packet.PayloadSize = event.Message.Size()

				event.Err = self.transport.Write(&packet, func(buffer []byte) error {
					_, err := event.Message.MarshalTo(buffer)
					return err
				})

				if event.Err == nil {
					self.transport.ShrinkOutputBuffer()
				}
			}
		}
	case EventRequest:
		self.filterEvent(event)

		if event.Err == nil {
			requestHeader := &event.RequestHeader
			requestHeaderSize := requestHeader.Size()
			packet.PayloadSize = 4 + requestHeaderSize + event.Message.Size()

			event.Err = self.transport.Write(&packet, func(buffer []byte) error {
				binary.BigEndian.PutUint32(buffer, uint32(requestHeaderSize))
				requestHeader.MarshalTo(buffer[4:])
				_, err := event.Message.MarshalTo(buffer[4+requestHeaderSize:])
				return err
			})
		}

		messageEmitter.PostEmitRequest(event)
	case EventResponse:
		self.filterEvent(event)

		if event.Err == nil {
			responseHeader := &event.ResponseHeader
			responseHeaderSize := responseHeader.Size()
			packet.PayloadSize = 4 + responseHeaderSize + event.Message.Size()

			event.Err = self.transport.Write(&packet, func(buffer []byte) error {
				binary.BigEndian.PutUint32(buffer, uint32(responseHeaderSize))
				responseHeader.MarshalTo(buffer[4:])
				_, err := event.Message.MarshalTo(buffer[4+responseHeaderSize:])
				return err
			})
		}

		messageEmitter.PostEmitResponse(event)
	case EventHangup:
		self.filterEvent(event)

		if event.Err == nil {
			hangup := &event.Hangup
			packet.PayloadSize = hangup.Size()

			event.Err = self.transport.Write(&packet, func(buffer []byte) error {
				hangup.MarshalTo(buffer)
				return nil
			})
		}
	default:
		panic("unreachable code")
	}

	if err := event.Err; err != nil {
		if err == ErrEventDropped {
			err = nil
		}

		return false, err
	}

	return true, nil
}

func (self *Stream) flush(ctx context.Context, timeout time.Duration, trafficEncrypter transport.TrafficEncrypter) error {
	return self.transport.Flush(ctx, timeout, trafficEncrypter)
}

func (self *Stream) filterEvent(event *Event) {
	eventFilters := self.options.DoGetEventFilters(event.direction, event.type_)

	for _, eventFilter := range eventFilters {
		eventFilter(event)

		if event.Err != nil {
			break
		}
	}
}

func (self *Stream) isHungUp() bool {
	return atomic.LoadInt32(&self.isHungUp_) == 1
}

type PendingRequest struct {
	ListNode   list.ListNode
	Header     proto.RequestHeader
	Underlying Message
}

type PendingResponse struct {
	ListNode   list.ListNode
	Header     proto.ResponseHeader
	Underlying Message
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

type pendingMessages struct {
	List       list.List
	ListLength int
}

var errBadEvent = errors.New("gogorpc/stream: bad event")

var (
	pendingRequestPool  = sync.Pool{New: func() interface{} { return new(PendingRequest) }}
	pendingResponsePool = sync.Pool{New: func() interface{} { return new(PendingResponse) }}
)
