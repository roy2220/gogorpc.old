package stream

import (
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/let-z-go/pbrpc/internal/transport"
)

type Options struct {
	IncomingKeepaliveInterval time.Duration
	OutgoingKeepaliveInterval time.Duration
	LocalConcurrencyLimit     int
	RemoteConcurrencyLimit    int
	Transport                 *transport.Options

	normalizeOnce sync.Once
	logger        *zerolog.Logger
}

func (self *Options) Normalize() *Options {
	self.normalizeOnce.Do(func() {
		normalizeDurValue(&self.IncomingKeepaliveInterval, defaultKeepaliveInterval, minKeepaliveInterval, maxKeepaliveInterval)
		normalizeDurValue(&self.OutgoingKeepaliveInterval, defaultKeepaliveInterval, minKeepaliveInterval, maxKeepaliveInterval)
		normalizeIntValue(&self.LocalConcurrencyLimit, defaultConcurrencyLimit, minConcurrencyLimit, maxConcurrencyLimit)
		normalizeIntValue(&self.RemoteConcurrencyLimit, defaultConcurrencyLimit, minConcurrencyLimit, maxConcurrencyLimit)

		if self.Transport == nil {
			self.Transport = &defaultTransportOptions
		}

		self.logger = self.Transport.Normalize().Logger
	})

	return self
}

func (self *Options) Logger() *zerolog.Logger {
	return self.logger
}

const (
	defaultKeepaliveInterval = 5 * time.Second
	minKeepaliveInterval     = 3 * time.Second
	maxKeepaliveInterval     = 60 * time.Second
)

const (
	defaultConcurrencyLimit = 1 << 17
	minConcurrencyLimit     = 1
	maxConcurrencyLimit     = 1 << 20
)

var defaultTransportOptions transport.Options

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
