package registry

import (
	"sync"

	"github.com/rs/zerolog"

	"github.com/let-z-go/gogorpc/client"
)

type Options struct {
	Client           *client.Options
	Logger           *zerolog.Logger
	BasicServiceMeta map[string]string
	BasicServiceTags []string

	normalizeOnce sync.Once
}

func (self *Options) Normalize() *Options {
	self.normalizeOnce.Do(func() {
		if self.Client == nil {
			self.Client = &defaultClientOptions
		}

		self.Client.Normalize()

		if self.Logger == nil {
			self.Logger = self.Client.Logger
		}
	})

	return self
}

var defaultClientOptions client.Options
