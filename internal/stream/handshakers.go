package stream

import (
	"context"
	"encoding/binary"
	"errors"
	"time"

	"github.com/let-z-go/pbrpc/internal/protocol"
	"github.com/let-z-go/pbrpc/internal/transport"
)

type Handshaker interface {
	NewHandshake() (handshakePayload Message)
	HandleHandshake(ctx context.Context, handshakePayload Message) (ok bool, err error)
	EmitHandshake() (handshakePayload Message, err error)
}

var ErrBadHandshake = errors.New("pbrpc/stream: bad handshake")

type passiveHandshaker struct {
	Inner Handshaker

	*stream

	handshakeHeader     protocol.StreamHandshakeHeader
	handshakeHeaderSize int
	handshakePayload    Message
	err                 error
}

var _ = transport.Handshaker(&passiveHandshaker{})

func (self *passiveHandshaker) HandleHandshake(ctx context.Context, rawHandshake []byte) (bool, error) {
	handshakeSize := len(rawHandshake)

	if handshakeSize < 4 {
		return false, ErrBadHandshake
	}

	handshakeHeaderSize := int(int32(binary.BigEndian.Uint32(rawHandshake)))
	handshakePayloadOffset := 4 + handshakeHeaderSize

	if handshakeHeaderSize < 0 || handshakePayloadOffset > handshakeSize {
		return false, ErrBadHandshake
	}

	if self.handshakeHeader.Unmarshal(rawHandshake[4:handshakePayloadOffset]) != nil {
		return false, ErrBadHandshake
	}

	self.options.Logger.Info().
		Str("side", "server-side").
		Int("size", handshakeSize).
		Int("header_size", handshakeHeaderSize).
		Str("transport_id", self.GetTransportID().String()).
		Int32("incoming_keepalive_interval", self.handshakeHeader.IncomingKeepaliveInterval).
		Int32("outgoing_keepalive_interval", self.handshakeHeader.OutgoingKeepaliveInterval).
		Int32("local_concurrency_limit", self.handshakeHeader.LocalConcurrencyLimit).
		Int32("remote_concurrency_limit", self.handshakeHeader.RemoteConcurrencyLimit).
		Msg("stream_incoming_handshake")
	handshakePayload := self.Inner.NewHandshake()

	if err := handshakePayload.Unmarshal(rawHandshake[handshakePayloadOffset:]); err != nil {
		return false, err
	}

	ok, err := self.Inner.HandleHandshake(ctx, handshakePayload)

	if err != nil {
		return false, err
	}

	maxKeepaliveIntervalMs := int32(maxKeepaliveInterval / time.Millisecond)

	if keepaliveIntervalMs := int32(self.options.OutgoingKeepaliveInterval / time.Millisecond); self.handshakeHeader.IncomingKeepaliveInterval < keepaliveIntervalMs {
		self.handshakeHeader.IncomingKeepaliveInterval = keepaliveIntervalMs
	} else if self.handshakeHeader.IncomingKeepaliveInterval > maxKeepaliveIntervalMs {
		self.handshakeHeader.IncomingKeepaliveInterval = maxKeepaliveIntervalMs
	}

	if keepaliveIntervalMs := int32(self.options.IncomingKeepaliveInterval / time.Millisecond); self.handshakeHeader.OutgoingKeepaliveInterval < keepaliveIntervalMs {
		self.handshakeHeader.OutgoingKeepaliveInterval = keepaliveIntervalMs
	} else if self.handshakeHeader.OutgoingKeepaliveInterval > maxKeepaliveIntervalMs {
		self.handshakeHeader.OutgoingKeepaliveInterval = maxKeepaliveIntervalMs
	}

	if int(self.handshakeHeader.LocalConcurrencyLimit) < minConcurrencyLimit {
		self.handshakeHeader.LocalConcurrencyLimit = minConcurrencyLimit
	} else if int(self.handshakeHeader.LocalConcurrencyLimit) > self.options.RemoteConcurrencyLimit {
		self.handshakeHeader.LocalConcurrencyLimit = int32(self.options.RemoteConcurrencyLimit)
	}

	if int(self.handshakeHeader.RemoteConcurrencyLimit) < minConcurrencyLimit {
		self.handshakeHeader.RemoteConcurrencyLimit = minConcurrencyLimit
	} else if int(self.handshakeHeader.RemoteConcurrencyLimit) > self.options.LocalConcurrencyLimit {
		self.handshakeHeader.RemoteConcurrencyLimit = int32(self.options.LocalConcurrencyLimit)
	}

	self.incomingKeepaliveInterval = time.Duration(self.handshakeHeader.OutgoingKeepaliveInterval) * time.Millisecond
	self.outgoingKeepaliveInterval = time.Duration(self.handshakeHeader.IncomingKeepaliveInterval) * time.Millisecond
	self.localConcurrencyLimit = int(self.handshakeHeader.RemoteConcurrencyLimit)
	self.remoteConcurrencyLimit = int(self.handshakeHeader.LocalConcurrencyLimit)
	return ok, nil
}

func (self *passiveHandshaker) SizeHandshake() int {
	self.handshakeHeaderSize = self.handshakeHeader.Size()
	self.handshakePayload, self.err = self.Inner.EmitHandshake()

	if self.err != nil {
		return 0
	}

	return 4 + self.handshakeHeaderSize + self.handshakePayload.Size()
}

func (self *passiveHandshaker) EmitHandshake(buffer []byte) error {
	if self.err != nil {
		return self.err
	}

	self.options.Logger.Info().
		Str("side", "server-side").
		Int("size", len(buffer)).
		Int("header_size", self.handshakeHeaderSize).
		Str("transport_id", self.GetTransportID().String()).
		Int32("incoming_keepalive_interval", self.handshakeHeader.IncomingKeepaliveInterval).
		Int32("outgoing_keepalive_interval", self.handshakeHeader.OutgoingKeepaliveInterval).
		Int32("local_concurrency_limit", self.handshakeHeader.LocalConcurrencyLimit).
		Int32("remote_concurrency_limit", self.handshakeHeader.RemoteConcurrencyLimit).
		Msg("stream_outgoing_handshake")
	binary.BigEndian.PutUint32(buffer, uint32(self.handshakeHeaderSize))
	self.handshakeHeader.MarshalTo(buffer[4:])
	_, err := self.handshakePayload.MarshalTo(buffer[4+self.handshakeHeaderSize:])
	return err
}

type activeHandshaker struct {
	Inner Handshaker

	*stream

	handshakeHeader     protocol.StreamHandshakeHeader
	handshakeHeaderSize int
	handshakePayload    Message
	err                 error
}

var _ = transport.Handshaker(&passiveHandshaker{})

func (self *activeHandshaker) SizeHandshake() int {
	self.handshakeHeader = protocol.StreamHandshakeHeader{
		IncomingKeepaliveInterval: int32(self.options.IncomingKeepaliveInterval / time.Millisecond),
		OutgoingKeepaliveInterval: int32(self.options.OutgoingKeepaliveInterval / time.Millisecond),
		LocalConcurrencyLimit:     int32(self.options.LocalConcurrencyLimit),
		RemoteConcurrencyLimit:    int32(self.options.RemoteConcurrencyLimit),
	}

	self.handshakeHeaderSize = self.handshakeHeader.Size()
	self.handshakePayload, self.err = self.Inner.EmitHandshake()

	if self.err != nil {
		return 0
	}

	return 4 + self.handshakeHeaderSize + self.handshakePayload.Size()
}

func (self *activeHandshaker) EmitHandshake(buffer []byte) error {
	if self.err != nil {
		return self.err
	}

	self.options.Logger.Info().
		Str("side", "client-side").
		Int("size", len(buffer)).
		Int("header_size", self.handshakeHeaderSize).
		Str("transport_id", self.GetTransportID().String()).
		Int32("incoming_keepalive_interval", self.handshakeHeader.IncomingKeepaliveInterval).
		Int32("outgoing_keepalive_interval", self.handshakeHeader.OutgoingKeepaliveInterval).
		Int32("local_concurrency_limit", self.handshakeHeader.LocalConcurrencyLimit).
		Int32("remote_concurrency_limit", self.handshakeHeader.RemoteConcurrencyLimit).
		Msg("stream_outgoing_handshake")
	binary.BigEndian.PutUint32(buffer, uint32(self.handshakeHeaderSize))
	self.handshakeHeader.MarshalTo(buffer[4:])
	_, err := self.handshakePayload.MarshalTo(buffer[4+self.handshakeHeaderSize:])
	return err
}

func (self *activeHandshaker) HandleHandshake(ctx context.Context, rawHandshake []byte) (bool, error) {
	handshakeSize := len(rawHandshake)

	if handshakeSize < 4 {
		return false, ErrBadHandshake
	}

	handshakeHeaderSize := int(int32(binary.BigEndian.Uint32(rawHandshake)))
	handshakePayloadOffset := 4 + handshakeHeaderSize

	if handshakeHeaderSize < 0 || handshakePayloadOffset > handshakeSize {
		return false, ErrBadHandshake
	}

	self.handshakeHeader.Reset()

	if self.handshakeHeader.Unmarshal(rawHandshake[4:handshakePayloadOffset]) != nil {
		return false, ErrBadHandshake
	}

	self.options.Logger.Info().
		Str("side", "client-side").
		Int("size", handshakeSize).
		Int("header_size", handshakeHeaderSize).
		Str("transport_id", self.GetTransportID().String()).
		Int32("incoming_keepalive_interval", self.handshakeHeader.IncomingKeepaliveInterval).
		Int32("outgoing_keepalive_interval", self.handshakeHeader.OutgoingKeepaliveInterval).
		Int32("local_concurrency_limit", self.handshakeHeader.LocalConcurrencyLimit).
		Int32("remote_concurrency_limit", self.handshakeHeader.RemoteConcurrencyLimit).
		Msg("stream_incoming_handshake")
	handshakePayload := self.Inner.NewHandshake()

	if err := handshakePayload.Unmarshal(rawHandshake[handshakePayloadOffset:]); err != nil {
		return false, err
	}

	ok, err := self.Inner.HandleHandshake(ctx, handshakePayload)

	if err != nil {
		return false, err
	}

	self.incomingKeepaliveInterval = time.Duration(self.handshakeHeader.IncomingKeepaliveInterval) * time.Millisecond
	self.outgoingKeepaliveInterval = time.Duration(self.handshakeHeader.OutgoingKeepaliveInterval) * time.Millisecond
	self.localConcurrencyLimit = int(self.handshakeHeader.LocalConcurrencyLimit)
	self.remoteConcurrencyLimit = int(self.handshakeHeader.RemoteConcurrencyLimit)
	return ok, nil
}
