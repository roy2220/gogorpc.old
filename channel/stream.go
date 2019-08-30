package channel

import (
	"github.com/let-z-go/pbrpc/internal/stream"
)

type (
	StreamOptions = stream.Options
	Message       = stream.Message
	RawMessage    = stream.RawMessage
	Handshaker    = stream.Handshaker
)

var (
	NullMessage = stream.NullMessage
)
