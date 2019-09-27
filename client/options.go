package client

import (
	"sync"
	"time"

	"github.com/let-z-go/gogorpc/channel"
	"github.com/rs/zerolog"
)

type Options struct {
	Channel             *channel.Options
	Logger              *zerolog.Logger
	ConnectTimeout      time.Duration
	CloseOnChannelError bool
	WithoutConnectRetry bool
	ConnectRetry        ConnectRetryOptions

	normalizeOnce sync.Once
}

func (self *Options) Normalize() *Options {
	self.normalizeOnce.Do(func() {
		if self.Channel == nil {
			self.Channel = &defaultChannelOptions
		}

		self.Channel.Normalize()

		if self.Logger == nil {
			self.Logger = self.Channel.Logger
		}

		if self.ConnectTimeout == 0 {
			self.ConnectTimeout = defaultConnectTimeout
		}

		if !self.WithoutConnectRetry {
			self.ConnectRetry.normalize()
		}
	})

	return self
}

type ConnectRetryOptions struct {
	MaxCount             int
	WithoutBackoff       bool
	MinBackoff           time.Duration
	MaxBackoff           time.Duration
	WithoutBackoffJitter bool
}

func (self *ConnectRetryOptions) normalize() {
	if self.WithoutBackoff {
		return
	}

	normalizeDurValue(&self.MinBackoff, defaultMinConnectRetryBackoff, minConnectRetryBackoff, maxConnectRetryBackoff)
	normalizeDurValue(&self.MaxBackoff, defaultMaxConnectRetryBackoff, minConnectRetryBackoff, maxConnectRetryBackoff)

	if self.MaxBackoff < self.MinBackoff {
		self.MaxBackoff = self.MinBackoff
	}
}

const defaultConnectTimeout = 3 * time.Second

const (
	defaultMinConnectRetryBackoff = 100 * time.Millisecond
	defaultMaxConnectRetryBackoff = 10 * time.Second
	minConnectRetryBackoff        = 10 * time.Millisecond
	maxConnectRetryBackoff        = 100 * time.Second
)

var defaultChannelOptions channel.Options

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
