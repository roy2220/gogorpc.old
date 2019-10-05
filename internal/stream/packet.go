package stream

import (
	"errors"
	"strconv"

	"github.com/gogo/protobuf/proto"

	"github.com/let-z-go/gogorpc/internal/protocol"
)

const (
	Incoming = Direction(iota)
	Outgoing

	NumberOfDirections = iota
)

const (
	MessageKeepalive = protocol.MESSAGE_KEEPALIVE
	MessageRequest   = protocol.MESSAGE_REQUEST
	MessageResponse  = protocol.MESSAGE_RESPONSE
	MessageHangup    = protocol.MESSAGE_HANGUP

	NumberOfMessageTypes = 4
)

type Packet struct {
	RequestHeader  protocol.RequestHeader
	ResponseHeader protocol.ResponseHeader
	Message        Message
	Hangup         protocol.Hangup
	Err            error

	direction   Direction
	messageType MessageType
}

func (self *Packet) Direction() Direction {
	return self.direction
}

func (self *Packet) MessageType() MessageType {
	return self.messageType
}

type Direction int

func (self Direction) String() string {
	switch self {
	case Incoming:
		return "INCOMING"
	case Outgoing:
		return "OUTGOING"
	default:
		return strconv.Itoa(int(self))
	}
}

type MessageType = protocol.MessageType

type Message interface {
	proto.Unmarshaler
	proto.Sizer

	MarshalTo([]byte) (int, error)
}

type PacketFilter func(packet *Packet) bool

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

var ErrPacketDropped = errors.New("gogorpc/stream: packet dropped")

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
