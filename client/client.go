package client

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/let-z-go/toolkit/timerpool"

	"github.com/let-z-go/gogorpc/channel"
)

type Client struct {
	options       *Options
	channel       channel.Channel
	rawServerURLs []string
	ctx           context.Context
	cancel        context.CancelFunc
	shutdown      chan struct{}
	lastError     atomic.Value
}

func (self *Client) Init(options *Options, rawServerURLs ...string) *Client {
	self.options = options.Normalize()
	self.channel.Init(false, self.options.Channel)
	self.rawServerURLs = rawServerURLs
	self.ctx, self.cancel = context.WithCancel(context.Background())
	self.shutdown = make(chan struct{})
	go self.run()
	return self
}

func (self *Client) Close() {
	self.cancel()
}

func (self *Client) DoRPC(rpc *channel.RPC, responseFactory channel.MessageFactory) {
	self.channel.DoRPC(rpc, responseFactory)
}

func (self *Client) PrepareRPC(rpc *channel.RPC, responseFactory channel.MessageFactory) {
	self.channel.PrepareRPC(rpc, responseFactory)
}

func (self *Client) Abort(extraData channel.ExtraData) {
	self.channel.Abort(extraData)
}

func (self *Client) Shutdown() <-chan struct{} {
	return self.shutdown
}

func (self *Client) LastError() error {
	value := self.lastError.Load()

	if value == nil {
		return nil
	}

	return value.(error)
}

func (self *Client) run() (err error) {
	defer func() {
		self.options.Logger.Error().Err(err).
			Strs("server_urls", self.rawServerURLs).
			Msg("client_closed")
		self.channel.Close()
		self.cancel()
		self.lastError.Store(err)
		close(self.shutdown)
	}()

	serverURLManager_ := serverURLManager{
		Options: self.options,
	}

	err = serverURLManager_.LoadServerURLs(self.rawServerURLs)

	if err != nil {
		return
	}

	connectRetryCount := -1

	for {
		err = self.ctx.Err()

		if err != nil {
			return
		}

		connectRetryCount++

		if connectRetryCount >= 1 && self.options.WithoutConnectRetry {
			return
		}

		var serverURL *url.URL
		serverURL, err = serverURLManager_.GetNextServerURL(self.ctx, connectRetryCount)

		if err != nil {
			return
		}

		var connector Connector
		connector, err = GetConnector(serverURL.Scheme)

		if err != nil {
			return
		}

		var connection net.Conn
		connection, err = connector(self.ctx, self.getConnectTimeout(), serverURL)

		if err != nil {
			self.options.Logger.Error().Err(err).
				Str("server_url", serverURL.String()).
				Msg("client_connect_failed")
			continue
		}

		err = self.channel.Run(self.ctx, serverURL, connection)
		self.options.Logger.Error().Err(err).
			Str("server_url", serverURL.String()).
			Str("transport_id", self.channel.TransportID().String()).
			Msg("client_channel_run_failed")

		if _, ok := err.(*channel.NetworkError); ok {
			continue
		}

		if hangup, ok := err.(*channel.Hangup); ok && !hangup.IsPassive {
			return
		}

		if self.options.CloseOnChannelError {
			return
		}
	}
}

func (self *Client) getConnectTimeout() time.Duration {
	if connectTimeout := self.options.ConnectTimeout; connectTimeout >= 1 {
		return connectTimeout
	}

	return 0
}

var (
	ErrNoValidServerURL      = errors.New("gogorpc/client: no valid server url")
	ErrTooManyConnectRetries = errors.New("gogorpc/client: too many connect retries")
)

type serverURLManager struct {
	Options *Options

	serverURLs          []*url.URL
	nextServerURLIndex  int
	connectRetryBackoff time.Duration
}

func (self *serverURLManager) LoadServerURLs(rawServerURLs []string) error {
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
		return ErrNoValidServerURL
	}

	rand.Shuffle(n, func(i, j int) {
		self.serverURLs[i], self.serverURLs[j] = self.serverURLs[j], self.serverURLs[i]
	})

	return nil
}

func (self *serverURLManager) GetNextServerURL(ctx context.Context, connectRetryCount int) (*url.URL, error) {
	connectRetryOptions := &self.Options.ConnectRetry

	if connectRetryOptions.MaxCount >= 1 && connectRetryCount > connectRetryOptions.MaxCount {
		return nil, ErrTooManyConnectRetries
	}

	if !connectRetryOptions.WithoutBackoff && connectRetryCount >= 1 {
		connectRetryBackoff := self.connectRetryBackoff

		if connectRetryCount == 1 {
			connectRetryBackoff = connectRetryOptions.MinBackoff
		} else {
			connectRetryBackoff *= 2

			if connectRetryBackoff > connectRetryOptions.MaxBackoff {
				connectRetryBackoff = connectRetryOptions.MaxBackoff
			}
		}

		self.connectRetryBackoff = connectRetryBackoff

		if !connectRetryOptions.WithoutBackoffJitter {
			connectRetryBackoff = time.Duration(float64(connectRetryBackoff) * (0.5 + rand.Float64()))
		}

		timer := timerpool.GetTimer(connectRetryBackoff)

		select {
		case <-ctx.Done():
			timerpool.StopAndPutTimer(timer)
			return nil, ctx.Err()
		case <-timer.C:
			timerpool.PutTimer(timer)
		}
	}

	serverURL := self.serverURLs[self.nextServerURLIndex]
	self.nextServerURLIndex = (self.nextServerURLIndex + 1) % len(self.serverURLs)
	return serverURL, nil
}
