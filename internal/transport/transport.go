package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/let-z-go/toolkit/bytestream"
	"github.com/let-z-go/toolkit/connection"
	"github.com/let-z-go/toolkit/uuid"
	"github.com/rs/zerolog"

	"github.com/let-z-go/gogorpc/internal/protocol"
)

type Transport struct {
	options               *Options
	id                    uuid.UUID
	connection            connection.Connection
	inputByteStream       bytestream.ByteStream
	outputByteStream      bytestream.ByteStream
	maxIncomingPacketSize int
	maxOutgoingPacketSize int
	peekedInputDataSize   int
}

func (self *Transport) Init(options *Options, id uuid.UUID) *Transport {
	self.options = options.Normalize()
	self.id = id
	return self
}

func (self *Transport) Close() error {
	if self.connection.IsClosed() {
		return nil
	}

	return self.connection.Close()
}

func (self *Transport) PostAccept(ctx context.Context, rawConnection net.Conn, handshaker Handshaker) (bool, error) {
	clientAddress := rawConnection.RemoteAddr().String()
	self.options.Logger.Info().
		Str("client_address", clientAddress).
		Dur("handshake_timeout", self.options.HandshakeTimeout).
		Int("min_input_buffer_size", self.options.MinInputBufferSize).
		Int("max_input_buffer_size", self.options.MaxInputBufferSize).
		Msg("transport_post_accepting")
	self.connection.Init(rawConnection)
	self.inputByteStream.ReserveBuffer(self.options.MinInputBufferSize)
	deadline := makeDeadline(self.options.HandshakeTimeout)
	var handshakeHeader protocol.TransportHandshakeHeader

	ok, err := self.receiveHandshake(
		true,
		ctx,
		deadline,
		&handshakeHeader,
		handshaker.HandleHandshake,
		clientAddress,
	)

	if err != nil {
		self.connection.Close()
		return false, err
	}

	if int(handshakeHeader.MaxIncomingPacketSize) < minMaxPacketSize {
		handshakeHeader.MaxIncomingPacketSize = minMaxPacketSize
	} else if int(handshakeHeader.MaxIncomingPacketSize) > self.options.MaxOutgoingPacketSize {
		handshakeHeader.MaxIncomingPacketSize = int32(self.options.MaxOutgoingPacketSize)
	}

	if int(handshakeHeader.MaxOutgoingPacketSize) < minMaxPacketSize {
		handshakeHeader.MaxOutgoingPacketSize = minMaxPacketSize
	} else if int(handshakeHeader.MaxOutgoingPacketSize) > self.options.MaxIncomingPacketSize {
		handshakeHeader.MaxOutgoingPacketSize = int32(self.options.MaxIncomingPacketSize)
	}

	self.maxIncomingPacketSize = int(handshakeHeader.MaxOutgoingPacketSize)
	self.maxOutgoingPacketSize = int(handshakeHeader.MaxIncomingPacketSize)

	if err := self.sendHandshake(
		true,
		ctx,
		deadline,
		&handshakeHeader,
		handshaker.SizeHandshake(),
		handshaker.EmitHandshake,
		clientAddress,
	); err != nil {
		self.connection.Close()
		return false, err
	}

	return ok, nil
}

func (self *Transport) PostConnect(ctx context.Context, rawConnection net.Conn, handshaker Handshaker) (bool, error) {
	serverAddress := rawConnection.RemoteAddr().String()
	self.options.Logger.Info().
		Str("server_address", serverAddress).
		Dur("handshake_timeout", self.options.HandshakeTimeout).
		Int("min_input_buffer_size", self.options.MinInputBufferSize).
		Int("max_input_buffer_size", self.options.MaxInputBufferSize).
		Msg("transport_post_connecting")
	self.connection.Init(rawConnection)
	self.inputByteStream.ReserveBuffer(self.options.MinInputBufferSize)
	deadline := makeDeadline(self.options.HandshakeTimeout)

	handshakeHeader := protocol.TransportHandshakeHeader{
		Id: protocol.UUID{
			Low:  self.id[0],
			High: self.id[1],
		},

		MaxIncomingPacketSize: int32(self.options.MaxIncomingPacketSize),
		MaxOutgoingPacketSize: int32(self.options.MaxOutgoingPacketSize),
	}

	if err := self.sendHandshake(
		false,
		ctx,
		deadline,
		&handshakeHeader,
		handshaker.SizeHandshake(),
		handshaker.EmitHandshake,
		serverAddress,
	); err != nil {
		self.connection.Close()
		return false, err
	}

	ok, err := self.receiveHandshake(
		false,
		ctx,
		deadline,
		&handshakeHeader,
		handshaker.HandleHandshake,
		serverAddress,
	)

	if err != nil {
		self.connection.Close()
		return false, err
	}

	self.maxIncomingPacketSize = int(handshakeHeader.MaxIncomingPacketSize)
	self.maxOutgoingPacketSize = int(handshakeHeader.MaxOutgoingPacketSize)
	return ok, nil
}

func (self *Transport) Peek(ctx context.Context, timeout time.Duration, packet *Packet) error {
	connectionIsPreRead := false

	if self.inputByteStream.GetDataSize() < 8 {
		self.connection.PreRead(ctx, makeDeadline(timeout))
		connectionIsPreRead = true

		for {
			dataSize, err := self.connection.DoRead(ctx, self.inputByteStream.GetBuffer())

			if err != nil {
				return &NetworkError{err}
			}

			self.inputByteStream.CommitBuffer(dataSize)

			if self.inputByteStream.GetDataSize() >= 8 {
				break
			}
		}
	}

	packetSize := int(int32(binary.BigEndian.Uint32(self.inputByteStream.GetData())))

	if packetSize < 8 {
		return ErrBadPacket
	}

	if packetSize > self.maxIncomingPacketSize {
		return ErrPacketTooLarge
	}

	if bufferSize := packetSize - self.inputByteStream.GetDataSize(); bufferSize >= 1 {
		self.inputByteStream.ReserveBuffer(bufferSize)

		if !connectionIsPreRead {
			self.connection.PreRead(ctx, makeDeadline(timeout))
		}

		for {
			dataSize, err := self.connection.DoRead(ctx, self.inputByteStream.GetBuffer())

			if err != nil {
				return &NetworkError{err}
			}

			self.inputByteStream.CommitBuffer(dataSize)

			if self.inputByteStream.GetDataSize() >= packetSize {
				break
			}
		}
	}

	rawPacket := self.inputByteStream.GetData()[:packetSize]
	packetHeaderSize := int(int32(binary.BigEndian.Uint32(rawPacket[4:])))
	packetPayloadOffset := 8 + packetHeaderSize

	if packetHeaderSize < 0 || packetPayloadOffset > packetSize {
		return ErrBadPacket
	}

	packet.Header.Reset()

	if packet.Header.Unmarshal(rawPacket[8:packetPayloadOffset]) != nil {
		return ErrBadPacket
	}

	packet.Payload = rawPacket[packetPayloadOffset:]
	self.peekedInputDataSize += packetSize
	return nil
}

func (self *Transport) PeekNext(packet *Packet) (bool, error) {
	data := self.inputByteStream.GetData()[self.peekedInputDataSize:]
	dataSize := len(data)

	if dataSize < 8 {
		self.skip()
		return false, nil
	}

	packetSize := int(int32(binary.BigEndian.Uint32(data)))

	if packetSize < 8 {
		return false, ErrBadPacket
	}

	if packetSize > self.maxIncomingPacketSize {
		return false, ErrPacketTooLarge
	}

	if packetSize > dataSize {
		self.skip()
		return false, nil
	}

	rawPacket := data[:packetSize]
	packetHeaderSize := int(int32(binary.BigEndian.Uint32(rawPacket[4:])))
	packetPayloadOffset := 8 + packetHeaderSize

	if packetHeaderSize < 0 || packetPayloadOffset > packetSize {
		return false, ErrBadPacket
	}

	packet.Header.Reset()

	if packet.Header.Unmarshal(rawPacket[8:packetPayloadOffset]) != nil {
		return false, ErrBadPacket
	}

	packet.Payload = rawPacket[packetPayloadOffset:]
	self.peekedInputDataSize += packetSize
	return true, nil
}

func (self *Transport) ShrinkInputBuffer() {
	self.inputByteStream.Shrink(self.options.MinInputBufferSize)
}

func (self *Transport) Write(packet *Packet, callback func([]byte) error) error {
	packetHeaderSize := packet.Header.Size()
	packetSize := 8 + packetHeaderSize + packet.PayloadSize

	if packetSize > self.maxOutgoingPacketSize {
		return ErrPacketTooLarge
	}

	if err := self.outputByteStream.WriteDirectly(packetSize, func(buffer []byte) error {
		binary.BigEndian.PutUint32(buffer, uint32(packetSize))
		binary.BigEndian.PutUint32(buffer[4:], uint32(packetHeaderSize))
		packet.Header.MarshalTo(buffer[8:])
		return callback(buffer[8+packetHeaderSize:])
	}); err != nil {
		return err
	}

	return nil
}

func (self *Transport) Flush(ctx context.Context, timeout time.Duration) error {
	data := self.outputByteStream.GetData()
	_, err := self.connection.Write(ctx, makeDeadline(timeout), data)
	self.outputByteStream.Skip(len(data))

	if err != nil {
		return &NetworkError{err}
	}

	return nil
}

func (self *Transport) ShrinkOutputBuffer() {
	self.outputByteStream.Shrink(0)
}

func (self *Transport) GetID() uuid.UUID {
	return self.id
}

func (self *Transport) receiveHandshake(
	isServerSide bool,
	ctx context.Context,
	deadline time.Time,
	handshakeHeader *protocol.TransportHandshakeHeader,
	handshakeHandler func(context.Context, []byte) (bool, error),
	peerAddress string,
) (bool, error) {
	self.connection.PreRead(ctx, deadline)

	for {
		dataSize, err := self.connection.DoRead(ctx, self.inputByteStream.GetBuffer())

		if err != nil {
			return false, &NetworkError{err}
		}

		self.inputByteStream.CommitBuffer(dataSize)

		if self.inputByteStream.GetDataSize() >= 8 {
			break
		}
	}

	handshakeSize := int(int32(binary.BigEndian.Uint32(self.inputByteStream.GetData())))

	if handshakeSize < 8 {
		return false, ErrBadHandshake
	}

	if handshakeSize > minMaxPacketSize {
		return false, ErrHandshakeTooLarge
	}

	if bufferSize := handshakeSize - self.inputByteStream.GetDataSize(); bufferSize >= 1 {
		self.inputByteStream.ReserveBuffer(bufferSize)

		for {
			dataSize, err := self.connection.DoRead(ctx, self.inputByteStream.GetBuffer())

			if err != nil {
				return false, &NetworkError{err}
			}

			self.inputByteStream.CommitBuffer(dataSize)

			if self.inputByteStream.GetDataSize() >= handshakeSize {
				break
			}
		}
	}

	rawHandshake := self.inputByteStream.GetData()[:handshakeSize]
	handshakeHeaderSize := int(int32(binary.BigEndian.Uint32(rawHandshake[4:])))
	handshakePayloadOffset := 8 + handshakeHeaderSize

	if handshakeHeaderSize < 0 || handshakePayloadOffset > handshakeSize {
		return false, ErrBadHandshake
	}

	handshakeHeader.Reset()

	if handshakeHeader.Unmarshal(rawHandshake[8:handshakePayloadOffset]) != nil {
		return false, ErrBadHandshake
	}

	var logEvent *zerolog.Event

	if isServerSide {
		logEvent = self.options.Logger.Info().Str("side", "server-side")

		if self.id.IsZero() {
			self.id = uuid.UUID{handshakeHeader.Id.Low, handshakeHeader.Id.High}
		} else {
			handshakeHeader.Id.Low, handshakeHeader.Id.High = self.id[0], self.id[1]
		}
	} else {
		logEvent = self.options.Logger.Info().Str("side", "client-side")
	}

	logEvent.Str("peer_address", peerAddress).
		Int("size", len(rawHandshake)).
		Int("header_size", handshakeHeaderSize).
		Str("id", self.id.String()).
		Int32("max_incoming_packet_size", handshakeHeader.MaxIncomingPacketSize).
		Int32("max_outgoing_packet_size", handshakeHeader.MaxOutgoingPacketSize).
		Msg("transport_incoming_handshake")
	ctx, cancel := context.WithDeadline(ctx, deadline)
	ok, err := handshakeHandler(ctx, rawHandshake[handshakePayloadOffset:])
	cancel()
	self.inputByteStream.Skip(handshakeSize)
	return ok, err
}

func (self *Transport) sendHandshake(
	isServerSide bool,
	ctx context.Context,
	deadline time.Time,
	handshakeHeader *protocol.TransportHandshakeHeader,
	handshakePayloadSize int,
	handshakeEmitter func([]byte) error,
	peerAddress string,
) error {
	var logEvent *zerolog.Event

	if isServerSide {
		logEvent = self.options.Logger.Info().Str("side", "server-side")
	} else {
		logEvent = self.options.Logger.Info().Str("side", "client-side")

		if self.id.IsZero() {
			self.id = uuid.GenerateUUID4Fast()
			handshakeHeader.Id.Low, handshakeHeader.Id.High = self.id[0], self.id[1]
		}
	}

	handshakeHeaderSize := handshakeHeader.Size()
	handshakeSize := 8 + handshakeHeaderSize + handshakePayloadSize
	logEvent.Str("peer_address", peerAddress).
		Int("size", handshakeSize).
		Int("header_size", handshakeHeaderSize).
		Str("id", self.id.String()).
		Int32("max_incoming_packet_size", handshakeHeader.MaxIncomingPacketSize).
		Int32("max_outgoing_packet_size", handshakeHeader.MaxOutgoingPacketSize).
		Msg("transport_outgoing_handshake")

	if handshakeSize > minMaxPacketSize {
		return ErrHandshakeTooLarge
	}

	if err := self.outputByteStream.WriteDirectly(handshakeSize, func(buffer []byte) error {
		binary.BigEndian.PutUint32(buffer, uint32(handshakeSize))
		binary.BigEndian.PutUint32(buffer[4:], uint32(handshakeHeaderSize))
		handshakeHeader.MarshalTo(buffer[8:])
		return handshakeEmitter(buffer[8+handshakeHeaderSize:])
	}); err != nil {
		return err
	}

	_, err := self.connection.Write(ctx, deadline, self.outputByteStream.GetData())
	self.outputByteStream.Skip(handshakeSize)

	if err != nil {
		return &NetworkError{err}
	}

	return nil
}

func (self *Transport) skip() {
	bufferIsInsufficient := self.inputByteStream.GetBufferSize() == 0
	self.inputByteStream.Skip(self.peekedInputDataSize)

	if bufferIsInsufficient {
		if self.inputByteStream.GetSize() < self.options.MaxInputBufferSize {
			self.inputByteStream.Expand()
		}
	}

	self.peekedInputDataSize = 0
}

type Handshaker interface {
	HandleHandshake(ctx context.Context, rawHandshake []byte) (ok bool, err error)
	SizeHandshake() (handshakSize int)
	EmitHandshake(buffer []byte) (err error)
}

type Packet struct {
	Header      protocol.PacketHeader
	Payload     []byte // only for peeking
	PayloadSize int    // only for writing
}

type NetworkError struct {
	Inner error
}

func (self *NetworkError) Error() string {
	return fmt.Sprintf("gogorpc/transport: network: %s", self.Inner.Error())
}

var (
	ErrHandshakeTooLarge = errors.New("gogorpc/transport: handshake too large")
	ErrBadHandshake      = errors.New("gogorpc/transport: bad handshake")
	ErrPacketTooLarge    = errors.New("gogorpc/transport: packet too large")
	ErrBadPacket         = errors.New("gogorpc/transport: bad packet")
)

func makeDeadline(timeout time.Duration) time.Time {
	if timeout < 1 {
		return time.Time{}
	} else {
		return time.Now().Add(timeout)
	}
}
