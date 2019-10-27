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

	"github.com/let-z-go/intrusive"
	"github.com/let-z-go/toolkit/deque"
	"github.com/let-z-go/toolkit/timerpool"
	"github.com/let-z-go/toolkit/uuid"

	"github.com/let-z-go/gogorpc/internal/proto"
	"github.com/let-z-go/gogorpc/internal/transport"
)

type Stream struct {
	options                   *Options
	transport                 transport.Transport
	userData                  interface{}
	dequeOfPendingRequests    *deque.Deque
	dequeOfPendingResponses   deque.Deque
	isHungUp_                 int32
	pendingHangup             chan *Hangup
	incomingKeepaliveInterval time.Duration
	outgoingKeepaliveInterval time.Duration
	incomingConcurrencyLimit  int
	outgoingConcurrencyLimit  int
	incomingConcurrency       int32
}

func (s *Stream) Init(
	options *Options,
	isServerSide bool,
	transportID uuid.UUID,
	userData interface{},
	dequeOfPendingRequests *deque.Deque,
) *Stream {
	s.options = options.Normalize()
	s.transport.Init(s.options.Transport, isServerSide, transportID)
	s.userData = userData
	s.dequeOfPendingRequests = dequeOfPendingRequests
	s.dequeOfPendingResponses.Init(0)
	s.pendingHangup = make(chan *Hangup, 1)
	return s
}

func (s *Stream) Close() error {
	err := s.transport.Close()
	listOfPendingResponses := deque.NewList()
	s.dequeOfPendingResponses.Close(listOfPendingResponses)
	putPooledPendingResponses(listOfPendingResponses)
	return err
}

func (s *Stream) Establish(ctx context.Context, connection net.Conn, handshaker Handshaker) (bool, error) {
	transportHandshaker_ := transportHandshaker{
		Underlying: handshaker,

		stream: s,
	}

	return s.transport.Establish(ctx, connection, &transportHandshaker_)
}

func (s *Stream) Process(
	ctx context.Context,
	trafficCrypter transport.TrafficCrypter,
	messageProcessor MessageProcessor,
) error {
	if err := s.prepare(trafficCrypter, messageProcessor); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	errs := make(chan error, 2)

	go func() {
		err := s.sendEvents(ctx, messageProcessor, trafficCrypter)
		errs <- err

		if _, ok := err.(*Hangup); ok {
			timer := timerpool.GetTimer(s.options.ActiveHangupTimeout)

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

	errs <- s.receiveEvents(ctx, trafficCrypter, messageProcessor, messageProcessor)
	cancel()
	err := <-errs
	<-errs
	return err
}

func (s *Stream) SendRequest(ctx context.Context, requestHeader *proto.RequestHeader, request Message) error {
	pendingRequest_ := pendingRequestPool.Get().(*pendingRequest)
	pendingRequest_.Header = *requestHeader
	pendingRequest_.Underlying = request

	// dequeOfPendingRequests.length += 1
	if err := s.dequeOfPendingRequests.AppendNode(ctx, &pendingRequest_.ListNode); err != nil {
		pendingRequestPool.Put(pendingRequest_)

		switch err {
		case deque.ErrDequeClosed:
			return ErrClosed
		default:
			return err
		}
	}

	return nil
}

func (s *Stream) SendResponse(responseHeader *proto.ResponseHeader, response Message) error {
	pendingResponse_ := pendingResponsePool.Get().(*pendingResponse)
	pendingResponse_.Header = *responseHeader
	pendingResponse_.Underlying = response

	// dequeOfPendingResponses.length += 1
	// dequeOfPendingResponses.capacity += 1
	if err := s.dequeOfPendingResponses.DiscardNodeRemoval(&pendingResponse_.ListNode, false); err != nil {
		pendingResponsePool.Put(pendingResponse_)

		switch err {
		case deque.ErrDequeClosed:
			return ErrClosed
		default:
			return err
		}
	}

	return nil
}

func (s *Stream) Abort(extraData ExtraData) {
	s.hangUp(HangupAborted, extraData)
}

func (s *Stream) IsServerSide() bool {
	return s.transport.IsServerSide()
}

func (s *Stream) TransportID() uuid.UUID {
	return s.transport.ID()
}

func (s *Stream) UserData() interface{} {
	return s.userData
}

func (s *Stream) prepare(trafficDecrypter transport.TrafficDecrypter, messageEmitter MessageEmitter) error {
	s.transport.Prepare(trafficDecrypter)

	switch n := s.outgoingConcurrencyLimit - s.dequeOfPendingRequests.Capacity(); {
	case n < 0:
		listOfPendingRequests := deque.NewList()
		// dequeOfPendingRequests.capacity += n
		s.dequeOfPendingRequests.Shrink(-n, false, listOfPendingRequests)
		getListNode := listOfPendingRequests.Underlying.GetNodesSafely()

		event := Event{
			stream:    s,
			direction: EventOutgoing,
			type_:     EventRequest,
		}

		err := error(nil)

		for listNode := getListNode(); listNode != nil; listNode = getListNode() {
			pendingRequest_ := (*pendingRequest)(listNode.GetContainer(unsafe.Offsetof(pendingRequest{}.ListNode)))
			event.RequestHeader = pendingRequest_.Header
			event.Message = pendingRequest_.Underlying
			event.Err = ErrTooManyOutgoingRequests
			messageEmitter.PostEmitRequest(&event)

			if event.Err != ErrEventDropped {
				if err == nil {
					err = event.Err
				} else {
					s.options.Logger.Error().Err(event.Err).
						Str("transport_id", s.TransportID().String()).
						Msg("stream_system_error")
				}

				continue
			}

			listNode.Remove()
			listOfPendingRequests.Length--
			listNode.Reset()
			pendingRequestPool.Put(pendingRequest_)
		}

		if err != nil {
			s.dequeOfPendingRequests.DiscardNodesRemoval(listOfPendingRequests, false)
		}

		return err
	case n == 0:
		return nil
	default: // n > 0
		// dequeOfPendingRequests.capacity += n
		s.dequeOfPendingRequests.CommitNodesRemoval(n)
		return nil
	}
}

func (s *Stream) receiveEvents(
	ctx context.Context,
	trafficDecrypter transport.TrafficDecrypter,
	messageFactory MessageFactory,
	messageHandler MessageHandler,
) error {
	event := Event{
		stream:    s,
		direction: EventIncoming,
	}

	for {
		if err := s.peek(
			ctx,
			s.incomingKeepaliveInterval*4/3,
			trafficDecrypter,
			messageFactory,
			&event,
		); err != nil {
			return err
		}

		oldIncomingConcurrency := int(atomic.LoadInt32(&s.incomingConcurrency))
		newIncomingConcurrency := oldIncomingConcurrency
		handledResponseCount := 0

		if err := s.handleEvent(
			ctx,
			&event,
			messageHandler,
			&newIncomingConcurrency,
			&handledResponseCount,
		); err != nil {
			return err
		}

		for {
			ok, err := s.peekNext(messageFactory, &event)

			if err != nil {
				return err
			}

			if !ok {
				break
			}

			if err := s.handleEvent(
				ctx,
				&event,
				messageHandler,
				&newIncomingConcurrency,
				&handledResponseCount,
			); err != nil {
				return err
			}
		}

		atomic.AddInt32(&s.incomingConcurrency, int32(newIncomingConcurrency-oldIncomingConcurrency))
		// dequeOfPendingRequests.capacity += handledResponseCount
		s.dequeOfPendingRequests.CommitNodesRemoval(handledResponseCount)
	}
}

func (s *Stream) peek(
	ctx context.Context,
	timeout time.Duration,
	trafficDecrypter transport.TrafficDecrypter,
	messageFactory MessageFactory,
	event *Event,
) error {
	var packet transport.Packet

	if err := s.transport.Peek(ctx, timeout, trafficDecrypter, &packet); err != nil {
		return err
	}

	s.loadEvent(event, &packet, messageFactory)
	return nil
}

func (s *Stream) peekNext(messageFactory MessageFactory, event *Event) (bool, error) {
	var packet transport.Packet
	ok, err := s.transport.PeekNext(&packet)

	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	s.loadEvent(event, &packet, messageFactory)
	return true, nil
}

func (s *Stream) loadEvent(event *Event, packet *transport.Packet, messageFactory MessageFactory) {
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
		if s.isHungUp() {
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

func (s *Stream) handleEvent(
	ctx context.Context,
	event *Event,
	messageHandler MessageHandler,
	incomingConcurrency *int,
	handledResponseCount *int,
) error {
	if event.Err == errBadEvent {
		s.hangUp(HangupBadIncomingEvent, nil)
		return nil
	}

	if event.Err == nil {
		s.filterEvent(event)
	}

	if event.Err == ErrEventDropped {
		return nil
	}

	switch event.type_ {
	case EventKeepalive:
		messageHandler.HandleKeepalive(ctx, event)

		if event.Err == nil {
			s.transport.ShrinkInputBuffer()
		}
	case EventRequest:
		if *incomingConcurrency == s.incomingConcurrencyLimit {
			s.hangUp(HangupTooManyIncomingRequests, nil)
			return nil
		}

		messageHandler.HandleRequest(ctx, event)
		*incomingConcurrency++
	case EventResponse:
		messageHandler.HandleResponse(ctx, event)
		*handledResponseCount++
	case EventHangup:
		if event.Err == nil {
			s.options.Logger.Info().Err(event.Err).
				Str("transport_id", s.TransportID().String()).
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
		s.options.Logger.Error().Err(event.Err).
			Str("transport_id", s.TransportID().String()).
			Msg("stream_system_error")
		s.hangUp(HangupSystem, nil)
	}

	return nil
}

func (s *Stream) hangUp(hangupCode HangupCode, extraData ExtraData) {
	if atomic.CompareAndSwapInt32(&s.isHungUp_, 0, 1) {
		s.pendingHangup <- &Hangup{
			IsPassive: false,
			Code:      hangupCode,
			ExtraData: extraData,
		}
	}
}

func (s *Stream) sendEvents(
	ctx context.Context,
	messageEmitter MessageEmitter,
	trafficEncrypter transport.TrafficEncrypter,
) error {
	ctx, cancel := context.WithCancel(ctx)
	errs := make(chan error, 2)
	pendingRequests := s.makePendingRequests(ctx, cancel, errs)
	pendingResponses := s.makePendingResponses(ctx, cancel, errs)

	event := Event{
		stream:    s,
		direction: EventOutgoing,
	}

	for {
		listOfPendingRequests, listOfPendingResponses, pendingHangup, err := s.checkPendingMessages(
			errs,
			pendingRequests,
			pendingResponses,
			s.outgoingKeepaliveInterval,
		)

		if err != nil {
			cancel()
			<-errs
			return err
		}

		err = s.emitEvents(listOfPendingRequests, listOfPendingResponses, pendingHangup, &event, messageEmitter)

		if err != nil {
			if listOfPendingRequests != nil {
				// dequeOfPendingRequests.length += listOfPendingRequests.Length
				// dequeOfPendingRequests.capacity += listOfPendingRequests.Length
				s.dequeOfPendingRequests.DiscardNodesRemoval(listOfPendingRequests, true)
			}

			if listOfPendingResponses != nil {
				// dequeOfPendingResponses.length += listOfPendingResponses.Length
				// dequeOfPendingResponses.capacity += listOfPendingResponses.Length
				s.dequeOfPendingResponses.DiscardNodesRemoval(listOfPendingResponses, true)
			}
		}

		if err2 := s.flush(ctx, s.outgoingKeepaliveInterval*4/3, trafficEncrypter); err2 != nil {
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

func (s *Stream) makePendingRequests(ctx context.Context, cancel context.CancelFunc, errs chan error) chan *deque.List {
	pendingRequests := make(chan *deque.List)

	go func() {
		errs <- s.putPendingRequests(ctx, pendingRequests)
		cancel()
	}()

	return pendingRequests
}

func (s *Stream) putPendingRequests(ctx context.Context, pendingRequests chan *deque.List) error {
	var lists [2]deque.List

	for i := 0; ; i ^= 1 {
		listOfPendingRequests := lists[i].Reset()
		// dequeOfPendingRequests.length -= listOfPendingRequests.Length
		// dequeOfPendingRequests.capacity -= listOfPendingRequests.Length
		err := s.dequeOfPendingRequests.RemoveNodes(ctx, true, listOfPendingRequests)

		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			// dequeOfPendingRequests.length += listOfPendingRequests.Length
			// dequeOfPendingRequests.capacity += listOfPendingRequests.Length
			s.dequeOfPendingRequests.DiscardNodesRemoval(listOfPendingRequests, true)
			return ctx.Err()
		case pendingRequests <- listOfPendingRequests:
		}
	}
}

func (s *Stream) makePendingResponses(ctx context.Context, cancel context.CancelFunc, errs chan error) chan *deque.List {
	pendingResponses := make(chan *deque.List)

	go func() {
		errs <- s.putPendingResponses(ctx, pendingResponses)
		cancel()
	}()

	return pendingResponses
}

func (s *Stream) putPendingResponses(ctx context.Context, pendingResponses chan *deque.List) error {
	var lists [2]deque.List

	for i := 0; ; i ^= 1 {
		listOfPendingResponses := lists[i].Reset()
		// dequeOfPendingResponses.length -= pendingResponses.Length
		// dequeOfPendingResponses.capacity -= pendingResponses.Length
		err := s.dequeOfPendingResponses.RemoveNodes(ctx, true, listOfPendingResponses)

		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			// dequeOfPendingResponses.length += pendingResponses.Length
			// dequeOfPendingResponses.capacity += pendingResponses.Length
			s.dequeOfPendingResponses.DiscardNodesRemoval(listOfPendingResponses, true)
			return ctx.Err()
		case pendingResponses <- listOfPendingResponses:
		}
	}
}

func (s *Stream) checkPendingMessages(
	errs chan error,
	pendingRequests chan *deque.List,
	pendingResponses chan *deque.List,
	timeout time.Duration,
) (*deque.List, *deque.List, *Hangup, error) {
	var listOfPendingRequests *deque.List
	var listOfPendingResponses *deque.List
	var pendingHangup *Hangup
	n := 0

	select {
	case listOfPendingRequests = <-pendingRequests:
		n++
	default:
	}

	select {
	case listOfPendingResponses = <-pendingResponses:
		n++
	default:
	}

	select {
	case pendingHangup = <-s.pendingHangup:
		n++
	default:
	}

	if n == 0 {
		timer := timerpool.GetTimer(timeout)

		select {
		case err := <-errs:
			timerpool.StopAndPutTimer(timer)
			return nil, nil, nil, err
		case listOfPendingRequests = <-pendingRequests:
			timerpool.StopAndPutTimer(timer)
		case listOfPendingResponses = <-pendingResponses:
			timerpool.StopAndPutTimer(timer)
		case pendingHangup = <-s.pendingHangup:
			timerpool.StopAndPutTimer(timer)
		case <-timer.C:
			timerpool.PutTimer(timer)
		}
	}

	return listOfPendingRequests, listOfPendingResponses, pendingHangup, nil
}

func (s *Stream) emitEvents(
	listOfPendingRequests *deque.List,
	listOfPendingResponses *deque.List,
	pendingHangup *Hangup,
	event *Event,
	messageEmitter MessageEmitter,
) error {
	err := error(nil)
	emittedEventCount := 0

	if listOfPendingRequests != nil {
		getListNode := listOfPendingRequests.Underlying.GetNodesSafely()
		event.type_ = EventRequest
		now := time.Now().UnixNano()
		droppedRequestCount := 0

		for listNode := getListNode(); listNode != nil; listNode = getListNode() {
			pendingRequest_ := (*pendingRequest)(listNode.GetContainer(unsafe.Offsetof(pendingRequest{}.ListNode)))
			event.RequestHeader = pendingRequest_.Header
			event.Message = pendingRequest_.Underlying
			var ok bool
			var err2 error

			if deadline := pendingRequest_.Header.Deadline; deadline != 0 && deadline <= now {
				event.Err = ErrRequestExpired
				messageEmitter.PostEmitRequest(event)
				ok, err2 = false, event.Err

				if err2 == ErrEventDropped {
					err2 = nil
				}
			} else {
				ok, err2 = s.write(event, messageEmitter)
			}

			if err2 != nil {
				if err == nil {
					err = err2
				} else {
					s.options.Logger.Error().Err(err2).
						Str("transport_id", s.TransportID().String()).
						Msg("stream_system_error")
				}

				continue
			}

			listNode.Remove()
			listOfPendingRequests.Length--
			listNode.Reset()
			pendingRequestPool.Put(pendingRequest_)

			if ok {
				emittedEventCount++
			} else {
				droppedRequestCount++
			}
		}

		// dequeOfPendingRequests.capacity += droppedRequestCount
		s.dequeOfPendingRequests.CommitNodesRemoval(droppedRequestCount)
	}

	if listOfPendingResponses != nil {
		getListNode := listOfPendingResponses.Underlying.GetNodesSafely()
		event.type_ = EventResponse
		emittedResponseCount := 0

		for listNode := getListNode(); listNode != nil; listNode = getListNode() {
			pendingResponse_ := (*pendingResponse)(listNode.GetContainer(unsafe.Offsetof(pendingResponse{}.ListNode)))
			event.ResponseHeader = pendingResponse_.Header
			event.Message = pendingResponse_.Underlying
			ok, err2 := s.write(event, messageEmitter)

			if err2 != nil {
				if err == nil {
					err = err2
				} else {
					s.options.Logger.Error().Err(err2).
						Str("transport_id", s.TransportID().String()).
						Msg("stream_system_error")
				}

				continue
			}

			listNode.Remove()
			listOfPendingResponses.Length--
			listNode.Reset()
			pendingResponsePool.Put(pendingResponse_)

			if ok {
				emittedEventCount++
				emittedResponseCount++
			}
		}

		atomic.AddInt32(&s.incomingConcurrency, -int32(emittedResponseCount))
	}

	if pendingHangup != nil {
		s.options.Logger.Info().
			Str("transport_id", s.TransportID().String()).
			Msg("stream_active_hangup")
		event.type_ = EventHangup
		event.Hangup.Code = pendingHangup.Code
		event.Hangup.ExtraData = pendingHangup.ExtraData

		if _, err := s.write(event, messageEmitter); err != nil {
			return err
		}

		return pendingHangup
	}

	if err == nil && emittedEventCount == 0 {
		event.type_ = EventKeepalive
		_, err = s.write(event, messageEmitter)
	}

	if err != nil {
		if err == transport.ErrPacketTooLarge {
			s.hangUp(HangupOutgoingPacketTooLarge, nil)
		} else {
			s.options.Logger.Error().Err(err).
				Str("transport_id", s.TransportID().String()).
				Msg("stream_system_error")
			s.hangUp(HangupSystem, nil)
		}
	}

	return nil
}

func (s *Stream) write(event *Event, messageEmitter MessageEmitter) (bool, error) {
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
			s.filterEvent(event)

			if event.Err == nil {
				packet.PayloadSize = event.Message.Size()

				event.Err = s.transport.Write(&packet, func(buffer []byte) error {
					_, err := event.Message.MarshalTo(buffer)
					return err
				})

				if event.Err == nil {
					s.transport.ShrinkOutputBuffer()
				}
			}
		}
	case EventRequest:
		s.filterEvent(event)

		if event.Err == nil {
			requestHeader := &event.RequestHeader
			requestHeaderSize := requestHeader.Size()
			packet.PayloadSize = 4 + requestHeaderSize + event.Message.Size()

			event.Err = s.transport.Write(&packet, func(buffer []byte) error {
				binary.BigEndian.PutUint32(buffer, uint32(requestHeaderSize))
				requestHeader.MarshalTo(buffer[4:])
				_, err := event.Message.MarshalTo(buffer[4+requestHeaderSize:])
				return err
			})
		}

		messageEmitter.PostEmitRequest(event)
	case EventResponse:
		s.filterEvent(event)

		if event.Err == nil {
			responseHeader := &event.ResponseHeader
			responseHeaderSize := responseHeader.Size()
			packet.PayloadSize = 4 + responseHeaderSize + event.Message.Size()

			event.Err = s.transport.Write(&packet, func(buffer []byte) error {
				binary.BigEndian.PutUint32(buffer, uint32(responseHeaderSize))
				responseHeader.MarshalTo(buffer[4:])
				_, err := event.Message.MarshalTo(buffer[4+responseHeaderSize:])
				return err
			})
		}

		messageEmitter.PostEmitResponse(event)
	case EventHangup:
		s.filterEvent(event)

		if event.Err == nil {
			hangup := &event.Hangup
			packet.PayloadSize = hangup.Size()

			event.Err = s.transport.Write(&packet, func(buffer []byte) error {
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

func (s *Stream) flush(ctx context.Context, timeout time.Duration, trafficEncrypter transport.TrafficEncrypter) error {
	return s.transport.Flush(ctx, timeout, trafficEncrypter)
}

func (s *Stream) filterEvent(event *Event) {
	eventFilters := s.options.DoGetEventFilters(event.direction, event.type_)

	for _, eventFilter := range eventFilters {
		eventFilter(event)

		if event.Err != nil {
			break
		}
	}
}

func (s *Stream) isHungUp() bool {
	return atomic.LoadInt32(&s.isHungUp_) == 1
}

var (
	ErrClosed                  = errors.New("gogorpc/stream: closed")
	ErrTooManyOutgoingRequests = errors.New("gogorpc/stream: too many outgoing requests")
	ErrRequestExpired          = errors.New("gogorpc/stream: request expired")
)

func PutPooledPendingRequests(listOfPendingRequests *deque.List) {
	getListNode := listOfPendingRequests.Underlying.GetNodesSafely()

	for listNode := getListNode(); listNode != nil; listNode = getListNode() {
		listNode.Remove()
		listNode.Reset()
		pendingRequest_ := (*pendingRequest)(listNode.GetContainer(unsafe.Offsetof(pendingRequest{}.ListNode)))
		pendingRequestPool.Put(pendingRequest_)
	}
}

type pendingRequest struct {
	ListNode   intrusive.ListNode
	Header     proto.RequestHeader
	Underlying Message
}

type pendingResponse struct {
	ListNode   intrusive.ListNode
	Header     proto.ResponseHeader
	Underlying Message
}

var errBadEvent = errors.New("gogorpc/stream: bad event")

var (
	pendingRequestPool  = sync.Pool{New: func() interface{} { return new(pendingRequest) }}
	pendingResponsePool = sync.Pool{New: func() interface{} { return new(pendingResponse) }}
)

func putPooledPendingResponses(listOfPendingResponses *deque.List) {
	getListNode := listOfPendingResponses.Underlying.GetNodesSafely()

	for listNode := getListNode(); listNode != nil; listNode = getListNode() {
		listNode.Remove()
		listNode.Reset()
		pendingResponse_ := (*pendingResponse)(listNode.GetContainer(unsafe.Offsetof(pendingResponse{}.ListNode)))
		pendingResponsePool.Put(pendingResponse_)
	}
}
