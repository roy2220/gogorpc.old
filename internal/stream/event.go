package stream

import (
	"context"
	"errors"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/let-z-go/toolkit/uuid"

	proto2 "github.com/let-z-go/gogorpc/internal/proto"
)

const (
	EventIncoming = EventDirection(iota)
	EventOutgoing

	NumberOfEventDirections = iota
)

const (
	EventKeepalive = proto2.EVENT_KEEPALIVE
	EventRequest   = proto2.EVENT_REQUEST
	EventResponse  = proto2.EVENT_RESPONSE
	EventHangup    = proto2.EVENT_HANGUP

	NumberOfEventTypes = 4
)

type Event struct {
	RequestHeader  proto2.RequestHeader
	ResponseHeader proto2.ResponseHeader
	Message        Message
	Hangup         proto2.Hangup
	Err            error

	stream    *Stream
	direction EventDirection
	type_     EventType
}

func (e *Event) Stream() RestrictedStream {
	return RestrictedStream{e.stream}
}

func (e *Event) Direction() EventDirection {
	return e.direction
}

func (e *Event) Type() EventType {
	return e.type_
}

type RestrictedStream struct {
	underlying *Stream
}

func (rs RestrictedStream) SendRequest(ctx context.Context, requestHeader *proto2.RequestHeader, request Message) error {
	return rs.underlying.SendRequest(ctx, requestHeader, request)
}

func (rs RestrictedStream) SendResponse(responseHeader *proto2.ResponseHeader, response Message) error {
	return rs.underlying.SendResponse(responseHeader, response)
}

func (rs RestrictedStream) Abort(extraData ExtraData) {
	rs.underlying.Abort(extraData)
}

func (rs RestrictedStream) IsServerSide() bool {
	return rs.underlying.IsServerSide()
}

func (rs RestrictedStream) TransportID() uuid.UUID {
	return rs.underlying.TransportID()
}

func (rs RestrictedStream) UserData() interface{} {
	return rs.underlying.UserData()
}

type EventDirection int

func (ed EventDirection) String() string {
	switch ed {
	case EventIncoming:
		return "EVENT_INCOMING"
	case EventOutgoing:
		return "EVENT_OUTGOING"
	default:
		return strconv.Itoa(int(ed))
	}
}

type EventType = proto2.EventType

type Message interface {
	proto.Unmarshaler
	proto.Sizer

	MarshalTo([]byte) (int, error)
}

type EventFilter func(event *Event)

type RawMessage []byte

var _ = Message((*RawMessage)(nil))

func (rm *RawMessage) Unmarshal(data []byte) error {
	*rm = make([]byte, len(data))
	copy(*rm, data)
	return nil
}

func (rm RawMessage) Size() int {
	return len(rm)
}

func (rm RawMessage) MarshalTo(buffer []byte) (int, error) {
	return copy(buffer, rm), nil
}

var ErrEventDropped = errors.New("gogorpc/stream: event dropped")

var NullMessage nullMessage

type nullMessage struct{}

func (nullMessage) Unmarshal([]byte) error {
	return nil
}

func (nullMessage) Size() int {
	return 0
}

func (nullMessage) MarshalTo([]byte) (int, error) {
	return 0, nil
}
