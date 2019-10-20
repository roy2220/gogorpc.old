package stream

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/rs/zerolog"

	"github.com/let-z-go/gogorpc/internal/proto"
	"github.com/let-z-go/gogorpc/internal/transport"
)

type Handshaker interface {
	NewHandshake() (handshakePayload Message)
	HandleHandshake(ctx context.Context, handshakePayload Message) (ok bool, err error)
	EmitHandshake() (handshakePayload Message, err error)
}

type DummyHandshaker struct{}

var _ = Handshaker(DummyHandshaker{})

func (DummyHandshaker) NewHandshake() Message                                  { return NullMessage }
func (DummyHandshaker) HandleHandshake(context.Context, Message) (bool, error) { return true, nil }
func (DummyHandshaker) EmitHandshake() (Message, error)                        { return NullMessage, nil }

var ErrBadHandshake = errors.New("gogorpc/stream: bad handshake")

type transportHandshaker struct {
	Underlying Handshaker

	stream              *Stream
	handshakeHeader     proto.StreamHandshakeHeader
	handshakeHeaderSize int
	handshakePayload    Message
	err                 error
}

var _ = transport.Handshaker(&transportHandshaker{})

func (self *transportHandshaker) HandleHandshake(ctx context.Context, rawHandshake []byte) (bool, error) {
	handshakeSize := len(rawHandshake)

	if handshakeSize < 4 {
		return false, ErrBadHandshake
	}

	handshakeHeaderSize := int(int32(binary.BigEndian.Uint32(rawHandshake)))
	handshakePayloadOffset := 4 + handshakeHeaderSize

	if handshakePayloadOffset < 4 || handshakePayloadOffset > handshakeSize {
		return false, ErrBadHandshake
	}

	self.handshakeHeader.Reset()

	if self.handshakeHeader.Unmarshal(rawHandshake[4:handshakePayloadOffset]) != nil {
		return false, ErrBadHandshake
	}

	var logEvent *zerolog.Event

	if self.stream.isServerSide {
		logEvent = self.stream.options.Logger.Info().Str("side", "server-side")
	} else {
		logEvent = self.stream.options.Logger.Info().Str("side", "client-side")
	}

	logEvent.Int("size", handshakeSize).
		Int("header_size", handshakeHeaderSize).
		Str("transport_id", self.stream.TransportID().String()).
		Int32("incoming_keepalive_interval", self.handshakeHeader.IncomingKeepaliveInterval).
		Int32("outgoing_keepalive_interval", self.handshakeHeader.OutgoingKeepaliveInterval).
		Int32("incoming_concurrency_limit", self.handshakeHeader.IncomingConcurrencyLimit).
		Int32("outgoing_concurrency_limit", self.handshakeHeader.OutgoingConcurrencyLimit).
		Msg("stream_incoming_handshake")
	handshakePayload := self.Underlying.NewHandshake()

	if err := handshakePayload.Unmarshal(rawHandshake[handshakePayloadOffset:]); err != nil {
		return false, err
	}

	ok, err := self.Underlying.HandleHandshake(ctx, handshakePayload)

	if err != nil {
		return false, err
	}

	if self.stream.isServerSide {
		maxKeepaliveIntervalMs := int32(maxKeepaliveInterval / time.Millisecond)

		if keepaliveIntervalMs := int32(self.stream.options.OutgoingKeepaliveInterval / time.Millisecond);
		/*   */ self.handshakeHeader.IncomingKeepaliveInterval < keepaliveIntervalMs {
			self.handshakeHeader.IncomingKeepaliveInterval = keepaliveIntervalMs
		} else if self.handshakeHeader.IncomingKeepaliveInterval > maxKeepaliveIntervalMs {
			self.handshakeHeader.IncomingKeepaliveInterval = maxKeepaliveIntervalMs
		}

		if keepaliveIntervalMs := int32(self.stream.options.IncomingKeepaliveInterval / time.Millisecond);
		/*   */ self.handshakeHeader.OutgoingKeepaliveInterval < keepaliveIntervalMs {
			self.handshakeHeader.OutgoingKeepaliveInterval = keepaliveIntervalMs
		} else if self.handshakeHeader.OutgoingKeepaliveInterval > maxKeepaliveIntervalMs {
			self.handshakeHeader.OutgoingKeepaliveInterval = maxKeepaliveIntervalMs
		}

		if int(self.handshakeHeader.IncomingConcurrencyLimit) < minConcurrencyLimit {
			self.handshakeHeader.IncomingConcurrencyLimit = minConcurrencyLimit
		} else if int(self.handshakeHeader.IncomingConcurrencyLimit) > self.stream.options.OutgoingConcurrencyLimit {
			self.handshakeHeader.IncomingConcurrencyLimit = int32(self.stream.options.OutgoingConcurrencyLimit)
		}

		if int(self.handshakeHeader.OutgoingConcurrencyLimit) < minConcurrencyLimit {
			self.handshakeHeader.OutgoingConcurrencyLimit = minConcurrencyLimit
		} else if int(self.handshakeHeader.OutgoingConcurrencyLimit) > self.stream.options.IncomingConcurrencyLimit {
			self.handshakeHeader.OutgoingConcurrencyLimit = int32(self.stream.options.IncomingConcurrencyLimit)
		}

		self.stream.incomingKeepaliveInterval = time.Duration(self.handshakeHeader.OutgoingKeepaliveInterval) * time.Millisecond
		self.stream.outgoingKeepaliveInterval = time.Duration(self.handshakeHeader.IncomingKeepaliveInterval) * time.Millisecond
		self.stream.incomingConcurrencyLimit = int(self.handshakeHeader.OutgoingConcurrencyLimit)
		self.stream.outgoingConcurrencyLimit = int(self.handshakeHeader.IncomingConcurrencyLimit)
	} else {
		self.stream.incomingKeepaliveInterval = time.Duration(self.handshakeHeader.IncomingKeepaliveInterval) * time.Millisecond
		self.stream.outgoingKeepaliveInterval = time.Duration(self.handshakeHeader.OutgoingKeepaliveInterval) * time.Millisecond
		self.stream.incomingConcurrencyLimit = int(self.handshakeHeader.IncomingConcurrencyLimit)
		self.stream.outgoingConcurrencyLimit = int(self.handshakeHeader.OutgoingConcurrencyLimit)
	}

	return ok, nil
}

func (self *transportHandshaker) SizeHandshake() int {
	if !self.stream.isServerSide {
		self.handshakeHeader = proto.StreamHandshakeHeader{
			IncomingKeepaliveInterval: int32(self.stream.options.IncomingKeepaliveInterval / time.Millisecond),
			OutgoingKeepaliveInterval: int32(self.stream.options.OutgoingKeepaliveInterval / time.Millisecond),
			IncomingConcurrencyLimit:  int32(self.stream.options.IncomingConcurrencyLimit),
			OutgoingConcurrencyLimit:  int32(self.stream.options.OutgoingConcurrencyLimit),
		}
	}

	self.handshakeHeaderSize = self.handshakeHeader.Size()
	self.handshakePayload, self.err = self.Underlying.EmitHandshake()

	if self.err != nil {
		return 0
	}

	return 4 + self.handshakeHeaderSize + self.handshakePayload.Size()
}

func (self *transportHandshaker) EmitHandshake(buffer []byte) error {
	if self.err != nil {
		return self.err
	}

	var logEvent *zerolog.Event

	if self.stream.isServerSide {
		logEvent = self.stream.options.Logger.Info().Str("side", "server-side")
	} else {
		logEvent = self.stream.options.Logger.Info().Str("side", "client-side")
	}

	logEvent.Int("size", len(buffer)).
		Int("header_size", self.handshakeHeaderSize).
		Str("transport_id", self.stream.TransportID().String()).
		Int32("incoming_keepalive_interval", self.handshakeHeader.IncomingKeepaliveInterval).
		Int32("outgoing_keepalive_interval", self.handshakeHeader.OutgoingKeepaliveInterval).
		Int32("incoming_concurrency_limit", self.handshakeHeader.IncomingConcurrencyLimit).
		Int32("outgoing_concurrency_limit", self.handshakeHeader.OutgoingConcurrencyLimit).
		Msg("stream_outgoing_handshake")
	binary.BigEndian.PutUint32(buffer, uint32(self.handshakeHeaderSize))
	self.handshakeHeader.MarshalTo(buffer[4:])
	_, err := self.handshakePayload.MarshalTo(buffer[4+self.handshakeHeaderSize:])
	return err
}
