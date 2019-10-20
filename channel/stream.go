package channel

import (
	"github.com/let-z-go/gogorpc/internal/stream"
)

const (
	EventIncoming  = stream.EventIncoming
	EventOutgoing  = stream.EventOutgoing
	EventKeepalive = stream.EventKeepalive
	EventRequest   = stream.EventRequest
	EventResponse  = stream.EventResponse
	EventHangup    = stream.EventHangup

	HangupAborted                 = stream.HangupAborted
	HangupBadIncomingEvent        = stream.HangupBadIncomingEvent
	HangupTooManyIncomingRequests = stream.HangupTooManyIncomingRequests
	HangupOutgoingPacketTooLarge  = stream.HangupOutgoingPacketTooLarge
	HangupSystem                  = stream.HangupSystem
)

type (
	StreamOptions = stream.Options

	Handshaker      = stream.Handshaker
	DummyHandshaker = stream.DummyHandshaker

	Event          = stream.Event
	EventDirection = stream.EventDirection
	EventType      = stream.EventType
	EventFilter    = stream.EventFilter
	Message        = stream.Message
	RawMessage     = stream.RawMessage

	Hangup     = stream.Hangup
	HangupCode = stream.HangupCode

	ExtraData    = stream.ExtraData
	ExtraDataRef = stream.ExtraDataRef
)

var (
	ErrBadHandshake = stream.ErrBadHandshake

	ErrEventDropped = stream.ErrEventDropped
	NullMessage     = stream.NullMessage
)
