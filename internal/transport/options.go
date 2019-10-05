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

func (self *Options) Normalize() *Options {
	self.normalizeOnce.Do(func() {
		if self.Logger == nil {
			self.Logger = &dummyLogger
		}

		normalizeDurValue(&self.HandshakeTimeout, defaultHandshakeTimeout, minHandshakeTimeout, maxHandshakeTimeout)
		normalizeIntValue(&self.MaxHandshakeSize, defaultMaxHandshakeSize, minMaxHandshakeSize, maxMaxHandshakeSize)
		normalizeIntValue(&self.MinInputBufferSize, defaultMinInputBufferSize, minInputBufferSize, maxInputBufferSize)
		self.MinInputBufferSize = int(utils.NextPowerOfTwo(int64(self.MinInputBufferSize)))
		normalizeIntValue(&self.MaxInputBufferSize, defaultMaxInputBufferSize, minInputBufferSize, maxInputBufferSize)
		self.MaxInputBufferSize = int(utils.NextPowerOfTwo(int64(self.MaxInputBufferSize)))

		if self.MaxInputBufferSize < self.MinInputBufferSize {
			self.MaxInputBufferSize = self.MinInputBufferSize
		}

		normalizeIntValue(&self.MaxIncomingPacketSize, defaultMaxPacketSize, minMaxPacketSize, maxMaxPacketSize)

		if self.MaxIncomingPacketSize > self.MaxInputBufferSize {
			self.MaxIncomingPacketSize = self.MaxInputBufferSize
		}

		normalizeIntValue(&self.MaxOutgoingPacketSize, defaultMaxPacketSize, minMaxPacketSize, maxMaxPacketSize)
	})

	return self
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
