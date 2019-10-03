package client

import (
	"context"
	"math/rand"
	"net/url"
	"time"

	"github.com/let-z-go/gogorpc/channel"
)

type Client struct {
	options       *Options
	channel       channel.Channel
	rawServerURLs []string
	ctx           context.Context
	cancel        context.CancelFunc
}

func (self *Client) Init(options *Options, rawServerURLs ...string) *Client {
	self.options = options.Normalize()
	self.channel.Init(false, self.options.Channel)
	self.rawServerURLs = rawServerURLs
	self.ctx, self.cancel = context.WithCancel(context.Background())
	go self.run()
	return self
}

func (self *Client) Close() {
	self.cancel()
}

func (self *Client) InvokeRPC(rpc *channel.RPC, responseFactory channel.MessageFactory) {
	self.channel.InvokeRPC(rpc, responseFactory)
}

func (self *Client) PrepareRPC(rpc *channel.RPC, responseFactory channel.MessageFactory) {
	self.channel.PrepareRPC(rpc, responseFactory)
}

func (self *Client) Abort(metadata channel.Metadata) {
	self.channel.Abort(metadata)
}

func (self *Client) run() {
	defer func() {
		self.options.Logger.Info().
			Strs("server_urls", self.rawServerURLs).
			Msg("client_closed")
		self.channel.Close()
		self.cancel()
	}()

	serverURLManager_ := serverURLManager{
		Options: self.options,
	}

	if !serverURLManager_.LoadServerURLs(self.rawServerURLs) {
		return
	}

	connectRetryCount := -1

	for {
		if self.ctx.Err() != nil {
			return
		}

		connectRetryCount++
		serverURL, ok := serverURLManager_.GetNextServerURL(self.ctx, connectRetryCount)

		if !ok {
			return
		}

		connector := MustGetConnector(serverURL.Scheme)
		connection, err := connector(self.ctx, self.getConnectTimeout(), serverURL)

		if err != nil {
			self.options.Logger.Error().Err(err).
				Str("server_url", serverURL.String()).
				Msg("client_connect_failed")
			continue
		}

		if err := self.channel.Establish(self.ctx, serverURL, connection); err != nil {
			self.options.Logger.Error().Err(err).
				Str("server_url", serverURL.String()).
				Str("transport_id", self.channel.TransportID().String()).
				Msg("client_channel_establish_failed")

			if _, ok := err.(*channel.NetworkError); ok {
				continue
			}

			if !self.options.CloseOnChannelError {
				continue
			}

			return
		}

		connectRetryCount = -1
		err = self.channel.Process(self.ctx)
		self.options.Logger.Error().Err(err).
			Str("server_url", serverURL.String()).
			Str("transport_id", self.channel.TransportID().String()).
			Msg("client_channel_process_failed")

		if _, ok = err.(*channel.NetworkError); ok {
			continue
		}

		if !self.options.CloseOnChannelError {
			continue
		}

		return
	}
}

func (self *Client) getConnectTimeout() time.Duration {
	if connectTimeout := self.options.ConnectTimeout; connectTimeout >= 1 {
		return connectTimeout
	}

	return 0
}

type serverURLManager struct {
	Options *Options

	serverURLs          []*url.URL
	nextServerURLIndex  int
	connectRetryBackoff time.Duration
}

func (self *serverURLManager) LoadServerURLs(rawServerURLs []string) bool {
	for _, rawServerURL := range rawServerURLs {
		serverURL, err := url.Parse(rawServerURL)

		if err != nil {
			self.Options.Logger.Warn().
				Err(err).
				Str("server_url", rawServerURL).
				Msg("client_invalid_server_url")
			continue
		}

		if _, err := GetConnector(serverURL.Scheme); err != nil {
			self.Options.Logger.Warn().
				Err(err).
				Str("server_url", rawServerURL).
				Msg("client_invalid_server_url")
			continue
		}

		self.serverURLs = append(self.serverURLs, serverURL)
	}

	n := len(self.serverURLs)

	if n == 0 {
		self.Options.Logger.Warn().Msg("client_no_valid_server_url")
		return false
	}

	rand.Shuffle(n, func(i, j int) {
		self.serverURLs[i], self.serverURLs[j] = self.serverURLs[j], self.serverURLs[i]
	})

	return true
}

func (self *serverURLManager) GetNextServerURL(ctx context.Context, connectRetryCount int) (*url.URL, bool) {
	if self.Options.WithoutConnectRetry {
		return nil, false
	}

	connectRetryPolicy := &self.Options.ConnectRetry

	if connectRetryPolicy.MaxCount >= 1 && connectRetryCount > connectRetryPolicy.MaxCount {
		self.Options.Logger.Error().
			Strs("valid_server_urls", self.getRawServerURLs()).
			Int("max_connect_retry_count", connectRetryPolicy.MaxCount).
			Msg("client_too_many_connect_retries")
		return nil, false
	}

	if !connectRetryPolicy.WithoutBackoff && connectRetryCount >= 1 {
		connectRetryBackoff := self.connectRetryBackoff

		if connectRetryCount == 1 {
			connectRetryBackoff = connectRetryPolicy.MinBackoff
		} else {
			connectRetryBackoff *= 2

			if connectRetryBackoff > connectRetryPolicy.MaxBackoff {
				connectRetryBackoff = connectRetryPolicy.MaxBackoff
			}
		}

		self.connectRetryBackoff = connectRetryBackoff

		if !connectRetryPolicy.WithoutBackoffJitter {
			connectRetryBackoff = time.Duration(float64(connectRetryBackoff) * (0.5 + rand.Float64()))
		}

		timer := time.NewTimer(connectRetryBackoff)

		select {
		case <-ctx.Done():
			timer.Stop()
			self.Options.Logger.Error().Err(ctx.Err()).
				Strs("valid_server_urls", self.getRawServerURLs()).
				Msg("client_connect_retry_delay_failed")
			return nil, false
		case <-timer.C:
		}
	}

	serverURL := self.serverURLs[self.nextServerURLIndex]
	self.nextServerURLIndex = (self.nextServerURLIndex + 1) % len(self.serverURLs)
	return serverURL, true
}

func (self *serverURLManager) getRawServerURLs() []string {
	rawServerURLs := make([]string, len(self.serverURLs))

	for i, serverURL := range self.serverURLs {
		rawServerURLs[i] = serverURL.String()
	}

	return rawServerURLs
}
