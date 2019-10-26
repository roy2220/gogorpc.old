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

	"github.com/let-z-go/gogorpc/internal/proto"
)

type Transport struct {
	options               *Options
	isServerSide          bool
	id                    uuid.UUID
	connection            connection.Connection
	inputByteStream       bytestream.ByteStream
	outputByteStream      bytestream.ByteStream
	maxIncomingPacketSize int
	maxOutgoingPacketSize int
	peekedTrafficSize     int
}

func (t *Transport) Init(options *Options, isServerSide bool, id uuid.UUID) *Transport {
	t.options = options.Normalize()
	t.isServerSide = isServerSide
	t.id = id
	return t
}

func (t *Transport) Close() error {
	if t.connection.IsClosed() {
		return nil
	}

	return t.connection.Close()
}

func (t *Transport) Establish(ctx context.Context, connection net.Conn, handshaker Handshaker) (bool, error) {
	var doEstablish func(*Transport, context.Context, net.Conn, Handshaker) (bool, error)

	if t.isServerSide {
		doEstablish = (*Transport).postAccept
	} else {
		doEstablish = (*Transport).postConnect
	}

	return doEstablish(t, ctx, connection, handshaker)
}

func (t *Transport) Prepare(trafficDecrypter TrafficDecrypter) {
	if traffic := t.inputByteStream.GetData(); len(traffic) >= 1 {
		trafficDecrypter.DecryptTraffic(traffic)
	}
}

func (t *Transport) Peek(ctx context.Context, timeout time.Duration, trafficDecrypter TrafficDecrypter, packet *Packet) error {
	traffic := t.inputByteStream.GetData()
	connectionIsPreRead := false

	if trafficSize := len(traffic); trafficSize < 8 {
		t.connection.PreRead(ctx, makeDeadline(timeout))
		connectionIsPreRead = true

		for {
			n, err := t.connection.DoRead(ctx, t.inputByteStream.GetBuffer())

			if err != nil {
				return &NetworkError{err}
			}

			t.inputByteStream.CommitBuffer(n)

			if t.inputByteStream.GetDataSize() >= 8 {
				break
			}
		}

		traffic = t.inputByteStream.GetData()
		trafficDecrypter.DecryptTraffic(traffic[trafficSize:])
	}

	packetSize := int(int32(binary.BigEndian.Uint32(traffic)))

	if packetSize < 8 {
		return ErrBadPacket
	}

	if packetSize > t.maxIncomingPacketSize {
		return ErrPacketTooLarge
	}

	if trafficSize := len(traffic); trafficSize < packetSize {
		t.inputByteStream.ReserveBuffer(packetSize - trafficSize)

		if !connectionIsPreRead {
			t.connection.PreRead(ctx, makeDeadline(timeout))
		}

		for {
			n, err := t.connection.DoRead(ctx, t.inputByteStream.GetBuffer())

			if err != nil {
				return &NetworkError{err}
			}

			t.inputByteStream.CommitBuffer(n)

			if t.inputByteStream.GetDataSize() >= packetSize {
				break
			}
		}

		traffic = t.inputByteStream.GetData()
		trafficDecrypter.DecryptTraffic(traffic[trafficSize:])
	}

	rawPacket := traffic[:packetSize]
	packetHeaderSize := int(int32(binary.BigEndian.Uint32(rawPacket[4:])))
	packetPayloadOffset := 8 + packetHeaderSize

	if packetPayloadOffset < 8 || packetPayloadOffset > packetSize {
		return ErrBadPacket
	}

	packet.Header.Reset()

	if packet.Header.Unmarshal(rawPacket[8:packetPayloadOffset]) != nil {
		return ErrBadPacket
	}

	packet.Payload = rawPacket[packetPayloadOffset:]
	t.peekedTrafficSize += packetSize
	return nil
}

func (t *Transport) PeekNext(packet *Packet) (bool, error) {
	traffic := t.inputByteStream.GetData()[t.peekedTrafficSize:]
	trafficSize := len(traffic)

	if trafficSize < 8 {
		t.skip()
		return false, nil
	}

	packetSize := int(int32(binary.BigEndian.Uint32(traffic)))

	if packetSize < 8 {
		return false, ErrBadPacket
	}

	if packetSize > t.maxIncomingPacketSize {
		return false, ErrPacketTooLarge
	}

	if packetSize > trafficSize {
		t.skip()
		return false, nil
	}

	rawPacket := traffic[:packetSize]
	packetHeaderSize := int(int32(binary.BigEndian.Uint32(rawPacket[4:])))
	packetPayloadOffset := 8 + packetHeaderSize

	if packetPayloadOffset < 8 || packetPayloadOffset > packetSize {
		return false, ErrBadPacket
	}

	packet.Header.Reset()

	if packet.Header.Unmarshal(rawPacket[8:packetPayloadOffset]) != nil {
		return false, ErrBadPacket
	}

	packet.Payload = rawPacket[packetPayloadOffset:]
	t.peekedTrafficSize += packetSize
	return true, nil
}

func (t *Transport) ShrinkInputBuffer() {
	t.inputByteStream.Shrink(t.options.MinInputBufferSize)
}

func (t *Transport) Write(packet *Packet, callback func([]byte) error) error {
	packetHeaderSize := packet.Header.Size()
	packetSize := 8 + packetHeaderSize + packet.PayloadSize

	if packetSize > t.maxOutgoingPacketSize {
		return ErrPacketTooLarge
	}

	if err := t.outputByteStream.WriteDirectly(packetSize, func(buffer []byte) error {
		binary.BigEndian.PutUint32(buffer, uint32(packetSize))
		binary.BigEndian.PutUint32(buffer[4:], uint32(packetHeaderSize))
		packet.Header.MarshalTo(buffer[8:])
		return callback(buffer[8+packetHeaderSize:])
	}); err != nil {
		return err
	}

	return nil
}

func (t *Transport) Flush(ctx context.Context, timeout time.Duration, trafficEncrypter TrafficEncrypter) error {
	traffic := t.outputByteStream.GetData()
	trafficEncrypter.EncryptTraffic(traffic)
	_, err := t.connection.Write(ctx, makeDeadline(timeout), traffic)
	t.outputByteStream.Skip(len(traffic))

	if err != nil {
		return &NetworkError{err}
	}

	return nil
}

func (t *Transport) ShrinkOutputBuffer() {
	t.outputByteStream.Shrink(0)
}

func (t *Transport) IsServerSide() bool {
	return t.isServerSide
}

func (t *Transport) ID() uuid.UUID {
	return t.id
}

func (t *Transport) postAccept(ctx context.Context, connection net.Conn, handshaker Handshaker) (bool, error) {
	clientAddress := connection.RemoteAddr().String()
	t.options.Logger.Info().
		Str("client_address", clientAddress).
		Dur("handshake_timeout", t.options.HandshakeTimeout).
		Int("min_input_buffer_size", t.options.MinInputBufferSize).
		Int("max_input_buffer_size", t.options.MaxInputBufferSize).
		Msg("transport_post_accepting")
	t.connection.Init(connection)
	t.inputByteStream.ReserveBuffer(t.options.MinInputBufferSize)
	deadline := makeDeadline(t.options.HandshakeTimeout)
	var handshakeHeader proto.TransportHandshakeHeader

	ok, err := t.receiveHandshake(
		ctx,
		deadline,
		&handshakeHeader,
		handshaker.HandleHandshake,
		clientAddress,
	)

	if err != nil {
		t.connection.Close()
		return false, err
	}

	if int(handshakeHeader.MaxIncomingPacketSize) < minMaxPacketSize {
		handshakeHeader.MaxIncomingPacketSize = minMaxPacketSize
	} else if int(handshakeHeader.MaxIncomingPacketSize) > t.options.MaxOutgoingPacketSize {
		handshakeHeader.MaxIncomingPacketSize = int32(t.options.MaxOutgoingPacketSize)
	}

	if int(handshakeHeader.MaxOutgoingPacketSize) < minMaxPacketSize {
		handshakeHeader.MaxOutgoingPacketSize = minMaxPacketSize
	} else if int(handshakeHeader.MaxOutgoingPacketSize) > t.options.MaxIncomingPacketSize {
		handshakeHeader.MaxOutgoingPacketSize = int32(t.options.MaxIncomingPacketSize)
	}

	t.maxIncomingPacketSize = int(handshakeHeader.MaxOutgoingPacketSize)
	t.maxOutgoingPacketSize = int(handshakeHeader.MaxIncomingPacketSize)

	if err := t.sendHandshake(
		ctx,
		deadline,
		&handshakeHeader,
		handshaker.SizeHandshake(),
		handshaker.EmitHandshake,
		clientAddress,
	); err != nil {
		t.connection.Close()
		return false, err
	}

	return ok, nil
}

func (t *Transport) postConnect(ctx context.Context, connection net.Conn, handshaker Handshaker) (bool, error) {
	serverAddress := connection.RemoteAddr().String()
	t.options.Logger.Info().
		Str("server_address", serverAddress).
		Dur("handshake_timeout", t.options.HandshakeTimeout).
		Int("min_input_buffer_size", t.options.MinInputBufferSize).
		Int("max_input_buffer_size", t.options.MaxInputBufferSize).
		Msg("transport_post_connecting")
	t.connection.Init(connection)
	t.inputByteStream.ReserveBuffer(t.options.MinInputBufferSize)
	deadline := makeDeadline(t.options.HandshakeTimeout)

	handshakeHeader := proto.TransportHandshakeHeader{
		Id: proto.UUID{
			Low:  t.id[0],
			High: t.id[1],
		},

		MaxIncomingPacketSize: int32(t.options.MaxIncomingPacketSize),
		MaxOutgoingPacketSize: int32(t.options.MaxOutgoingPacketSize),
	}

	if err := t.sendHandshake(
		ctx,
		deadline,
		&handshakeHeader,
		handshaker.SizeHandshake(),
		handshaker.EmitHandshake,
		serverAddress,
	); err != nil {
		t.connection.Close()
		return false, err
	}

	ok, err := t.receiveHandshake(
		ctx,
		deadline,
		&handshakeHeader,
		handshaker.HandleHandshake,
		serverAddress,
	)

	if err != nil {
		t.connection.Close()
		return false, err
	}

	t.maxIncomingPacketSize = int(handshakeHeader.MaxIncomingPacketSize)
	t.maxOutgoingPacketSize = int(handshakeHeader.MaxOutgoingPacketSize)
	return ok, nil
}

func (t *Transport) receiveHandshake(
	ctx context.Context,
	deadline time.Time,
	handshakeHeader *proto.TransportHandshakeHeader,
	handshakeHandler func(context.Context, []byte) (bool, error),
	peerAddress string,
) (bool, error) {
	t.connection.PreRead(ctx, deadline)

	for {
		n, err := t.connection.DoRead(ctx, t.inputByteStream.GetBuffer())

		if err != nil {
			return false, &NetworkError{err}
		}

		t.inputByteStream.CommitBuffer(n)

		if t.inputByteStream.GetDataSize() >= 8 {
			break
		}
	}

	traffic := t.inputByteStream.GetData()
	handshakeSize := int(int32(binary.BigEndian.Uint32(traffic)))

	if handshakeSize < 8 {
		return false, ErrBadHandshake
	}

	if handshakeSize > t.options.MaxHandshakeSize {
		return false, ErrHandshakeTooLarge
	}

	if trafficSize := len(traffic); trafficSize < handshakeSize {
		t.inputByteStream.ReserveBuffer(handshakeSize - trafficSize)

		for {
			n, err := t.connection.DoRead(ctx, t.inputByteStream.GetBuffer())

			if err != nil {
				return false, &NetworkError{err}
			}

			t.inputByteStream.CommitBuffer(n)

			if t.inputByteStream.GetDataSize() >= handshakeSize {
				break
			}
		}

		traffic = t.inputByteStream.GetData()
	}

	rawHandshake := traffic[:handshakeSize]
	handshakeHeaderSize := int(int32(binary.BigEndian.Uint32(rawHandshake[4:])))
	handshakePayloadOffset := 8 + handshakeHeaderSize

	if handshakePayloadOffset < 8 || handshakePayloadOffset > handshakeSize {
		return false, ErrBadHandshake
	}

	handshakeHeader.Reset()

	if handshakeHeader.Unmarshal(rawHandshake[8:handshakePayloadOffset]) != nil {
		return false, ErrBadHandshake
	}

	var logEvent *zerolog.Event

	if t.isServerSide {
		logEvent = t.options.Logger.Info().Str("side", "server-side")

		if t.id.IsZero() {
			t.id = uuid.UUID{handshakeHeader.Id.Low, handshakeHeader.Id.High}
		} else {
			handshakeHeader.Id = proto.UUID{
				Low:  t.id[0],
				High: t.id[1],
			}
		}
	} else {
		logEvent = t.options.Logger.Info().Str("side", "client-side")
	}

	logEvent.Str("peer_address", peerAddress).
		Int("size", len(rawHandshake)).
		Int("header_size", handshakeHeaderSize).
		Str("id", t.id.String()).
		Int32("max_incoming_packet_size", handshakeHeader.MaxIncomingPacketSize).
		Int32("max_outgoing_packet_size", handshakeHeader.MaxOutgoingPacketSize).
		Msg("transport_incoming_handshake")
	ctx, cancel := context.WithDeadline(ctx, deadline)
	ok, err := handshakeHandler(ctx, rawHandshake[handshakePayloadOffset:])
	cancel()
	t.inputByteStream.Skip(handshakeSize)
	return ok, err
}

func (t *Transport) sendHandshake(
	ctx context.Context,
	deadline time.Time,
	handshakeHeader *proto.TransportHandshakeHeader,
	handshakePayloadSize int,
	handshakeEmitter func([]byte) error,
	peerAddress string,
) error {
	var logEvent *zerolog.Event

	if t.isServerSide {
		logEvent = t.options.Logger.Info().Str("side", "server-side")
	} else {
		logEvent = t.options.Logger.Info().Str("side", "client-side")

		if t.id.IsZero() {
			t.id = uuid.GenerateUUID4Fast()

			handshakeHeader.Id = proto.UUID{
				Low:  t.id[0],
				High: t.id[1],
			}
		}
	}

	handshakeHeaderSize := handshakeHeader.Size()
	handshakeSize := 8 + handshakeHeaderSize + handshakePayloadSize
	logEvent.Str("peer_address", peerAddress).
		Int("size", handshakeSize).
		Int("header_size", handshakeHeaderSize).
		Str("id", t.id.String()).
		Int32("max_incoming_packet_size", handshakeHeader.MaxIncomingPacketSize).
		Int32("max_outgoing_packet_size", handshakeHeader.MaxOutgoingPacketSize).
		Msg("transport_outgoing_handshake")

	if handshakeSize > t.options.MaxHandshakeSize {
		return ErrHandshakeTooLarge
	}

	if err := t.outputByteStream.WriteDirectly(handshakeSize, func(buffer []byte) error {
		binary.BigEndian.PutUint32(buffer, uint32(handshakeSize))
		binary.BigEndian.PutUint32(buffer[4:], uint32(handshakeHeaderSize))
		handshakeHeader.MarshalTo(buffer[8:])
		return handshakeEmitter(buffer[8+handshakeHeaderSize:])
	}); err != nil {
		return err
	}

	_, err := t.connection.Write(ctx, deadline, t.outputByteStream.GetData())
	t.outputByteStream.Skip(handshakeSize)

	if err != nil {
		return &NetworkError{err}
	}

	return nil
}

func (t *Transport) skip() {
	bufferIsInsufficient := t.inputByteStream.GetBufferSize() == 0
	t.inputByteStream.Skip(t.peekedTrafficSize)

	if bufferIsInsufficient {
		if t.inputByteStream.Size() < t.options.MaxInputBufferSize {
			t.inputByteStream.Expand()
		}
	}

	t.peekedTrafficSize = 0
}

type Handshaker interface {
	HandleHandshake(ctx context.Context, handshakePayload []byte) (ok bool, err error)
	SizeHandshake() (handshakSize int)
	EmitHandshake(buffer []byte) (err error)
}

type Packet struct {
	Header      proto.PacketHeader
	Payload     []byte // only for peeking
	PayloadSize int    // only for writing
}

type NetworkError struct {
	Underlying error
}

func (ne *NetworkError) Error() string {
	return fmt.Sprintf("gogorpc/transport: network: %s", ne.Underlying.Error())
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
	}

	return time.Now().Add(timeout)
}
