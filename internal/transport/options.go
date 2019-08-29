package transport

import (
	"sync"
	"time"

	"github.com/rs/zerolog"
)

type Options struct {
	Logger                *zerolog.Logger
	Connector             Connector
	HandshakeTimeout      time.Duration
	MinInputBufferSize    int
	MaxInputBufferSize    int
	MaxIncomingPacketSize int
	MaxOutgoingPacketSize int

	normalizeOnce sync.Once
}

func (self *Options) Normalize() *Options {
	self.normalizeOnce.Do(func() {
		if self.Logger == nil {
			self.Logger = &defaultLogger
		}

		if self.Connector == nil {
			self.Connector = TCPConnector
		}

		normalizeDurValue(&self.HandshakeTimeout, defaultHandshakeTimeout, minHandshakeTimeout, maxHandshakeTimeout)
		normalizeIntValue(&self.MinInputBufferSize, defaultMinInputBufferSize, minInputBufferSize, maxInputBufferSize)
		normalizeIntValue(&self.MaxInputBufferSize, defaultMaxInputBufferSize, minInputBufferSize, maxInputBufferSize)

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
	maxHandshakeTimeout     = 30 * time.Second
)

const (
	defaultMaxPacketSize = 1 << 20
	minMaxPacketSize     = 1 << 12
	maxMaxPacketSize     = 1 << 30
)

const (
	defaultMinInputBufferSize = 1 << 12
	defaultMaxInputBufferSize = 1 << 24
	minInputBufferSize        = 1 << 10
	maxInputBufferSize        = 1 << 30
)

var defaultLogger = zerolog.Nop()

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
