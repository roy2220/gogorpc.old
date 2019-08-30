package channel

import (
	"github.com/let-z-go/pbrpc/internal/transport"
)

type (
	TransportOptions = transport.Options
)

var (
	TCPConnector       = transport.TCPConnector
	WebSocketConnector = transport.WebSocketConnector
)
