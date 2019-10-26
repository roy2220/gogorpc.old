package transport

import (
	"sync"
	"time"

	"github.com/let-z-go/toolkit/utils"
	"github.com/rs/zerolog"
)

type Options struct {
	Logger                *zerolog.Logger
	HandshakeTimeout      time.Duration
	MaxHandshakeSize      int
	MinInputBufferSize    int
	MaxInputBufferSize    int
	MaxIncomingPacketSize int
	MaxOutgoingPacketSize int

	normalizeOnce sync.Once
}

func (o *Options) Normalize() *Options {
	o.normalizeOnce.Do(func() {
		if o.Logger == nil {
			o.Logger = &dummyLogger
		}

		normalizeDurValue(&o.HandshakeTimeout, defaultHandshakeTimeout, minHandshakeTimeout, maxHandshakeTimeout)
		normalizeIntValue(&o.MaxHandshakeSize, defaultMaxHandshakeSize, minMaxHandshakeSize, maxMaxHandshakeSize)
		normalizeIntValue(&o.MinInputBufferSize, defaultMinInputBufferSize, minInputBufferSize, maxInputBufferSize)
		o.MinInputBufferSize = int(utils.NextPowerOfTwo(int64(o.MinInputBufferSize)))
		normalizeIntValue(&o.MaxInputBufferSize, defaultMaxInputBufferSize, minInputBufferSize, maxInputBufferSize)
		o.MaxInputBufferSize = int(utils.NextPowerOfTwo(int64(o.MaxInputBufferSize)))

		if o.MaxInputBufferSize < o.MinInputBufferSize {
			o.MaxInputBufferSize = o.MinInputBufferSize
		}

		normalizeIntValue(&o.MaxIncomingPacketSize, defaultMaxPacketSize, minMaxPacketSize, maxMaxPacketSize)

		if o.MaxIncomingPacketSize > o.MaxInputBufferSize {
			o.MaxIncomingPacketSize = o.MaxInputBufferSize
		}

		normalizeIntValue(&o.MaxOutgoingPacketSize, defaultMaxPacketSize, minMaxPacketSize, maxMaxPacketSize)
	})

	return o
}

const (
	defaultHandshakeTimeout = 3 * time.Second
	minHandshakeTimeout     = 1 * time.Second
	maxHandshakeTimeout     = 5 * time.Second
)

const (
	defaultMaxHandshakeSize = 1 << 16
	minMaxHandshakeSize     = 1 << 12
	maxMaxHandshakeSize     = 1 << 20
)

const (
	defaultMinInputBufferSize = 1 << 12
	defaultMaxInputBufferSize = 1 << 24
	minInputBufferSize        = 1 << 10
	maxInputBufferSize        = 1 << 30
)

const (
	defaultMaxPacketSize = 1 << 20
	minMaxPacketSize     = 1 << 10
	maxMaxPacketSize     = 1 << 30
)

var dummyLogger = zerolog.Nop()

func normalizeDurValue(value *time.Duration, defaultValue, minValue, maxValue time.Duration) {
	if *value == 0 {
		*value = defaultValue
		return
	}

	if *value < minValue {
		*value = minValue
		return
	}

	if *value > maxValue {
		*value = maxValue
		return
	}
}

func normalizeIntValue(value *int, defaultValue, minValue, maxValue int) {
	if *value == 0 {
		*value = defaultValue
		return
	}

	if *value < minValue {
		*value = minValue
		return
	}

	if *value > maxValue {
		*value = maxValue
		return
	}
}
