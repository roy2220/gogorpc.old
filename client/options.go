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

func (o *Options) Normalize() *Options {
	o.normalizeOnce.Do(func() {
		if o.Channel == nil {
			o.Channel = &defaultChannelOptions
		}

		o.Channel.Normalize()

		if o.Logger == nil {
			o.Logger = o.Channel.Logger
		}

		if o.ConnectTimeout == 0 {
			o.ConnectTimeout = defaultConnectTimeout
		}

		if !o.WithoutConnectRetry {
			o.ConnectRetry.normalize()
		}
	})

	return o
}

type ConnectRetryOptions struct {
	MaxCount             int
	WithoutBackoff       bool
	MinBackoff           time.Duration
	MaxBackoff           time.Duration
	WithoutBackoffJitter bool
}

func (cro *ConnectRetryOptions) normalize() {
	if cro.WithoutBackoff {
		return
	}

	normalizeDurValue(&cro.MinBackoff, defaultMinConnectRetryBackoff, minConnectRetryBackoff, maxConnectRetryBackoff)
	normalizeDurValue(&cro.MaxBackoff, defaultMaxConnectRetryBackoff, minConnectRetryBackoff, maxConnectRetryBackoff)

	if cro.MaxBackoff < cro.MinBackoff {
		cro.MaxBackoff = cro.MinBackoff
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
