package stream

import (
	"fmt"

	"github.com/let-z-go/gogorpc/internal/protocol"
)

const (
	HangupAborted                 = protocol.HANGUP_ABORTED
	HangupBadIncomingPacket       = protocol.HANGUP_BAD_INCOMING_PACKET
	HangupTooManyIncomingRequests = protocol.HANGUP_TOO_MANY_INCOMING_REQUESTS
	HangupOutgoingPacketTooLarge  = protocol.HANGUP_OUTGOING_PACKET_TOO_LARGE
	HangupSystem                  = protocol.HANGUP_SYSTEM
)

type Hangup struct {
	IsPassive bool
	Code      HangupCode
	ExtraData ExtraData
}

func (self *Hangup) Error() string {
	message := "gogorpc/stream: hangup"

	if self.IsPassive {
		message += " (passive): "
	} else {
		message += " (active): "
	}

	switch self.Code {
	case HangupAborted:
		message += "aborted"
	case HangupBadIncomingPacket:
		message += "bad incoming packet"
	case HangupTooManyIncomingRequests:
		message += "too many incoming requests"
	case HangupOutgoingPacketTooLarge:
		message += "outgoing packet too large"
	case HangupSystem:
		message += "system"
	default:
		message += fmt.Sprintf("hangup %d", self.Code)
	}

	return message
}

type HangupCode = protocol.HangupCode
