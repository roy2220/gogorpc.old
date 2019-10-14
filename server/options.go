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

func (self *Options) Normalize() *Options {
	self.normalizeOnce.Do(func() {
		if self.Channel == nil {
			self.Channel = &defaultChannelOptions
		}

		self.Channel.Normalize()

		if self.Logger == nil {
			self.Logger = self.Channel.Logger
		}
	})

	return self
}

func (self *Options) Do(doer func(*Options)) *Options {
	doer(self)
	return self
}

type Hook struct {
	BeforeRun func(ctx context.Context, url_ *url.URL) error
	AfterRun  func(url_ *url.URL)
}

var defaultChannelOptions channel.Options
