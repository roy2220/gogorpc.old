package stream

import (
	"errors"

	"github.com/gogo/protobuf/proto"

	"github.com/let-z-go/gogorpc/internal/protocol"
)

type Packet struct {
	RequestHeader  protocol.RequestHeader
	ResponseHeader protocol.ResponseHeader
	Message        Message
	Hangup         protocol.Hangup
	Err            error

	messageType protocol.MessageType
}

type Message interface {
	proto.Unmarshaler
	proto.Sizer

	MarshalTo([]byte) (int, error)
}

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
