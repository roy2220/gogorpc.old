package stream

import (
	"fmt"

	"github.com/let-z-go/pbrpc/internal/protocol"
)

const (
	ErrorAborted                 = protocol.ERROR_ABORTED
	ErrorBadIncomingPacket       = protocol.ERROR_BAD_INCOMING_PACKET
	ErrorTooManyIncomingRequests = protocol.ERROR_TOO_MANY_INCOMING_REQUESTS
	ErrorOutgoingPacketTooLarge  = protocol.ERROR_OUTGOING_PACKET_TOO_LARGE
	ErrorSystem                  = protocol.ERROR_SYSTEM
)

type Error struct {
	Code       ErrorCode
	IsFromWire bool
}

func (self *Error) Error() string {
	message := "pbrpc/stream: "

	switch self.Code {
	case ErrorAborted:
		message += "aborted"
	case ErrorBadIncomingPacket:
		message += "bad incoming packet"
	case ErrorTooManyIncomingRequests:
		message += "too many incoming requests"
	case ErrorOutgoingPacketTooLarge:
		message += "error outgoing packet too large"
	case ErrorSystem:
		message += "system"
	default:
		message += fmt.Sprintf("error %d", self.Code)
	}

	if self.IsFromWire {
		message += " (from wire)"
	}

	return message
}

type ErrorCode = protocol.ErrorCode
