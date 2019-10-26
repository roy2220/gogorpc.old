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

func (o *Options) Normalize() *Options {
	o.normalizeOnce.Do(func() {
		if o.Client == nil {
			o.Client = &defaultClientOptions
		}

		o.Client.Normalize()

		if o.Logger == nil {
			o.Logger = o.Client.Logger
		}
	})

	return o
}

var defaultClientOptions client.Options
