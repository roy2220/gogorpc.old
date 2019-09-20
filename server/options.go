package server

import (
	"sync"
	"time"

	"github.com/let-z-go/gogorpc/channel"
	"github.com/rs/zerolog"
)

type Options struct {
	Channel         *channel.Options
	Logger          *zerolog.Logger
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

var defaultChannelOptions channel.Options
