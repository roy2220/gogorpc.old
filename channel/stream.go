package channel

import (
	"github.com/let-z-go/gogorpc/internal/stream"
)

const (
	HangupAborted                 = stream.HangupAborted
	HangupBadIncomingPacket       = stream.HangupBadIncomingPacket
	HangupTooManyIncomingRequests = stream.HangupTooManyIncomingRequests
	HangupOutgoingPacketTooLarge  = stream.HangupOutgoingPacketTooLarge
	HangupSystem                  = stream.HangupSystem
)

type (
	StreamOptions = stream.Options

	Packet     = stream.Packet
	Message    = stream.Message
	RawMessage = stream.RawMessage

	Handshaker      = stream.Handshaker
	DummyHandshaker = stream.DummyHandshaker

	Hangup     = stream.Hangup
	HangupCode = stream.HangupCode
	Metadata   = stream.Metadata

	MessageFilter              = stream.MessageFilter
	IncomingMessageFilter      = stream.IncomingMessageFilter
	OutgoingMessageFilter      = stream.OutgoingMessageFilter
	KeepaliveFilter            = stream.KeepaliveFilter
	RequestFilter              = stream.RequestFilter
	ResponseFilter             = stream.ResponseFilter
	HangupFilter               = stream.HangupFilter
	DummyMessageFilter         = stream.DummyMessageFilter
	DummyIncomingMessageFilter = stream.DummyIncomingMessageFilter
	DummyOutgoingMessageFilter = stream.DummyOutgoingMessageFilter
	DummyKeepaliveFilter       = stream.DummyKeepaliveFilter
	DummyRequestFilter         = stream.DummyRequestFilter
	DummyResponseFilter        = stream.DummyResponseFilter
	DummyHangupFilter          = stream.DummyHangupFilter
)

var (
	ErrPacketDropped = stream.ErrPacketDropped
	NullMessage      = stream.NullMessage

	ErrBadHandshake = stream.ErrBadHandshake
)
