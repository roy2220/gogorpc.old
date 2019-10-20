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

func (self *Event) Stream() RestrictedStream {
	return RestrictedStream{self.stream}
}

func (self *Event) Direction() EventDirection {
	return self.direction
}

func (self *Event) Type() EventType {
	return self.type_
}

type RestrictedStream struct {
	underlying *Stream
}

func (self RestrictedStream) SendRequest(ctx context.Context, requestHeader *proto2.RequestHeader, request Message) error {
	return self.underlying.SendRequest(ctx, requestHeader, request)
}

func (self RestrictedStream) SendResponse(responseHeader *proto2.ResponseHeader, response Message) error {
	return self.underlying.SendResponse(responseHeader, response)
}

func (self RestrictedStream) Abort(extraData ExtraData) {
	self.underlying.Abort(extraData)
}

func (self RestrictedStream) IsServerSide() bool {
	return self.underlying.IsServerSide()
}

func (self RestrictedStream) UserData() interface{} {
	return self.underlying.UserData()
}

func (self RestrictedStream) TransportID() uuid.UUID {
	return self.underlying.TransportID()
}

type EventDirection int

func (self EventDirection) String() string {
	switch self {
	case EventIncoming:
		return "EVENT_INCOMING"
	case EventOutgoing:
		return "EVENT_OUTGOING"
	default:
		return strconv.Itoa(int(self))
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

func (self *RawMessage) Unmarshal(data []byte) error {
	*self = make([]byte, len(data))
	copy(*self, data)
	return nil
}

func (self RawMessage) Size() int {
	return len(self)
}

func (self RawMessage) MarshalTo(buffer []byte) (int, error) {
	return copy(buffer, self), nil
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
