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

func (th *transportHandshaker) HandleHandshake(ctx context.Context, rawHandshake []byte) (bool, error) {
	handshakeSize := len(rawHandshake)

	if handshakeSize < 4 {
		return false, ErrBadHandshake
	}

	handshakeHeaderSize := int(int32(binary.BigEndian.Uint32(rawHandshake)))
	handshakePayloadOffset := 4 + handshakeHeaderSize

	if handshakePayloadOffset < 4 || handshakePayloadOffset > handshakeSize {
		return false, ErrBadHandshake
	}

	th.handshakeHeader.Reset()

	if th.handshakeHeader.Unmarshal(rawHandshake[4:handshakePayloadOffset]) != nil {
		return false, ErrBadHandshake
	}

	var logEvent *zerolog.Event

	if th.stream.IsServerSide() {
		logEvent = th.stream.options.Logger.Info().Str("side", "server-side")
	} else {
		logEvent = th.stream.options.Logger.Info().Str("side", "client-side")
	}

	logEvent.Int("size", handshakeSize).
		Int("header_size", handshakeHeaderSize).
		Str("transport_id", th.stream.TransportID().String()).
		Int32("incoming_keepalive_interval", th.handshakeHeader.IncomingKeepaliveInterval).
		Int32("outgoing_keepalive_interval", th.handshakeHeader.OutgoingKeepaliveInterval).
		Int32("incoming_concurrency_limit", th.handshakeHeader.IncomingConcurrencyLimit).
		Int32("outgoing_concurrency_limit", th.handshakeHeader.OutgoingConcurrencyLimit).
		Msg("stream_incoming_handshake")
	handshakePayload := th.Underlying.NewHandshake()

	if err := handshakePayload.Unmarshal(rawHandshake[handshakePayloadOffset:]); err != nil {
		return false, err
	}

	ok, err := th.Underlying.HandleHandshake(ctx, handshakePayload)

	if err != nil {
		return false, err
	}

	if th.stream.IsServerSide() {
		maxKeepaliveIntervalMs := int32(maxKeepaliveInterval / time.Millisecond)

		if keepaliveIntervalMs := int32(th.stream.options.OutgoingKeepaliveInterval / time.Millisecond);
		/*   */ th.handshakeHeader.IncomingKeepaliveInterval < keepaliveIntervalMs {
			th.handshakeHeader.IncomingKeepaliveInterval = keepaliveIntervalMs
		} else if th.handshakeHeader.IncomingKeepaliveInterval > maxKeepaliveIntervalMs {
			th.handshakeHeader.IncomingKeepaliveInterval = maxKeepaliveIntervalMs
		}

		if keepaliveIntervalMs := int32(th.stream.options.IncomingKeepaliveInterval / time.Millisecond);
		/*   */ th.handshakeHeader.OutgoingKeepaliveInterval < keepaliveIntervalMs {
			th.handshakeHeader.OutgoingKeepaliveInterval = keepaliveIntervalMs
		} else if th.handshakeHeader.OutgoingKeepaliveInterval > maxKeepaliveIntervalMs {
			th.handshakeHeader.OutgoingKeepaliveInterval = maxKeepaliveIntervalMs
		}

		if int(th.handshakeHeader.IncomingConcurrencyLimit) < minConcurrencyLimit {
			th.handshakeHeader.IncomingConcurrencyLimit = minConcurrencyLimit
		} else if int(th.handshakeHeader.IncomingConcurrencyLimit) > th.stream.options.OutgoingConcurrencyLimit {
			th.handshakeHeader.IncomingConcurrencyLimit = int32(th.stream.options.OutgoingConcurrencyLimit)
		}

		if int(th.handshakeHeader.OutgoingConcurrencyLimit) < minConcurrencyLimit {
			th.handshakeHeader.OutgoingConcurrencyLimit = minConcurrencyLimit
		} else if int(th.handshakeHeader.OutgoingConcurrencyLimit) > th.stream.options.IncomingConcurrencyLimit {
			th.handshakeHeader.OutgoingConcurrencyLimit = int32(th.stream.options.IncomingConcurrencyLimit)
		}

		th.stream.incomingKeepaliveInterval = time.Duration(th.handshakeHeader.OutgoingKeepaliveInterval) * time.Millisecond
		th.stream.outgoingKeepaliveInterval = time.Duration(th.handshakeHeader.IncomingKeepaliveInterval) * time.Millisecond
		th.stream.incomingConcurrencyLimit = int(th.handshakeHeader.OutgoingConcurrencyLimit)
		th.stream.outgoingConcurrencyLimit = int(th.handshakeHeader.IncomingConcurrencyLimit)
	} else {
		th.stream.incomingKeepaliveInterval = time.Duration(th.handshakeHeader.IncomingKeepaliveInterval) * time.Millisecond
		th.stream.outgoingKeepaliveInterval = time.Duration(th.handshakeHeader.OutgoingKeepaliveInterval) * time.Millisecond
		th.stream.incomingConcurrencyLimit = int(th.handshakeHeader.IncomingConcurrencyLimit)
		th.stream.outgoingConcurrencyLimit = int(th.handshakeHeader.OutgoingConcurrencyLimit)
	}

	return ok, nil
}

func (th *transportHandshaker) SizeHandshake() int {
	if !th.stream.IsServerSide() {
		th.handshakeHeader = proto.StreamHandshakeHeader{
			IncomingKeepaliveInterval: int32(th.stream.options.IncomingKeepaliveInterval / time.Millisecond),
			OutgoingKeepaliveInterval: int32(th.stream.options.OutgoingKeepaliveInterval / time.Millisecond),
			IncomingConcurrencyLimit:  int32(th.stream.options.IncomingConcurrencyLimit),
			OutgoingConcurrencyLimit:  int32(th.stream.options.OutgoingConcurrencyLimit),
		}
	}

	th.handshakeHeaderSize = th.handshakeHeader.Size()
	th.handshakePayload, th.err = th.Underlying.EmitHandshake()

	if th.err != nil {
		return 0
	}

	return 4 + th.handshakeHeaderSize + th.handshakePayload.Size()
}

func (th *transportHandshaker) EmitHandshake(buffer []byte) error {
	if th.err != nil {
		return th.err
	}

	var logEvent *zerolog.Event

	if th.stream.IsServerSide() {
		logEvent = th.stream.options.Logger.Info().Str("side", "server-side")
	} else {
		logEvent = th.stream.options.Logger.Info().Str("side", "client-side")
	}

	logEvent.Int("size", len(buffer)).
		Int("header_size", th.handshakeHeaderSize).
		Str("transport_id", th.stream.TransportID().String()).
		Int32("incoming_keepalive_interval", th.handshakeHeader.IncomingKeepaliveInterval).
		Int32("outgoing_keepalive_interval", th.handshakeHeader.OutgoingKeepaliveInterval).
		Int32("incoming_concurrency_limit", th.handshakeHeader.IncomingConcurrencyLimit).
		Int32("outgoing_concurrency_limit", th.handshakeHeader.OutgoingConcurrencyLimit).
		Msg("stream_outgoing_handshake")
	binary.BigEndian.PutUint32(buffer, uint32(th.handshakeHeaderSize))
	th.handshakeHeader.MarshalTo(buffer[4:])
	_, err := th.handshakePayload.MarshalTo(buffer[4+th.handshakeHeaderSize:])
	return err
}
