package server

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/let-z-go/gogorpc/channel"
	"github.com/rs/zerolog"
)

type Options struct {
	Channel         *channel.Options
	Logger          *zerolog.Logger
	Hooks           []*Hook
	ShutdownTimeout time.Duration

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
	})

	return o
}

func (o *Options) Do(doer func(*Options)) *Options {
	doer(o)
	return o
}

type Hook struct {
	BeforeRun func(ctx context.Context, url_ *url.URL) error
	AfterRun  func(url_ *url.URL)
}

var defaultChannelOptions channel.Options
