package channel

import (
	"github.com/let-z-go/gogorpc/internal/stream"
)

const (
	Incoming         = stream.Incoming
	Outgoing         = stream.Outgoing
	MessageKeepalive = stream.MessageKeepalive
	MessageRequest   = stream.MessageRequest
	MessageResponse  = stream.MessageResponse
	MessageHangup    = stream.MessageHangup

	HangupAborted                 = stream.HangupAborted
	HangupBadIncomingPacket       = stream.HangupBadIncomingPacket
	HangupTooManyIncomingRequests = stream.HangupTooManyIncomingRequests
	HangupOutgoingPacketTooLarge  = stream.HangupOutgoingPacketTooLarge
	HangupSystem                  = stream.HangupSystem
)

type (
	StreamOptions = stream.Options

	Packet       = stream.Packet
	Direction    = stream.Direction
	MessageType  = stream.MessageType
	PacketFilter = stream.PacketFilter
	Message      = stream.Message
	RawMessage   = stream.RawMessage

	Handshaker      = stream.Handshaker
	DummyHandshaker = stream.DummyHandshaker

	Hangup     = stream.Hangup
	HangupCode = stream.HangupCode

	ExtraData    = stream.ExtraData
	ExtraDataRef = stream.ExtraDataRef
)

var (
	ErrPacketDropped = stream.ErrPacketDropped
	NullMessage      = stream.NullMessage

	ErrBadHandshake = stream.ErrBadHandshake
)
