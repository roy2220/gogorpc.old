package channel

import (
	"github.com/let-z-go/gogorpc/internal/transport"
)

type (
	NetworkError = transport.NetworkError

	TransportOptions = transport.Options

	TrafficCrypter      = transport.TrafficCrypter
	DummyTrafficCrypter = transport.DummyTrafficCrypter
)
