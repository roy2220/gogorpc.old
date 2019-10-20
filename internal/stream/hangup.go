package stream

import (
	"fmt"

	"github.com/let-z-go/gogorpc/internal/proto"
)

const (
	HangupAborted                 = proto.HANGUP_ABORTED
	HangupBadIncomingEvent        = proto.HANGUP_BAD_INCOMING_EVENT
	HangupTooManyIncomingRequests = proto.HANGUP_TOO_MANY_INCOMING_REQUESTS
	HangupOutgoingPacketTooLarge  = proto.HANGUP_OUTGOING_PACKET_TOO_LARGE
	HangupSystem                  = proto.HANGUP_SYSTEM
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
	case HangupBadIncomingEvent:
		message += "bad incoming event"
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

type HangupCode = proto.HangupCode
