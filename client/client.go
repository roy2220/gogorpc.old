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

func (c *Client) Init(options *Options, rawServerURLs ...string) *Client {
	c.options = options.Normalize()
	c.channel.Init(c.options.Channel, false)
	c.rawServerURLs = rawServerURLs
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.shutdown = make(chan struct{})
	go c.run()
	return c
}

func (c *Client) Close() {
	c.cancel()
}

func (c *Client) DoRPC(rpc *channel.RPC, responseFactory channel.MessageFactory) {
	c.channel.DoRPC(rpc, responseFactory)
}

func (c *Client) PrepareRPC(rpc *channel.RPC, responseFactory channel.MessageFactory) {
	c.channel.PrepareRPC(rpc, responseFactory)
}

func (c *Client) Abort(extraData channel.ExtraData) {
	c.channel.Abort(extraData)
}

func (c *Client) Shutdown() <-chan struct{} {
	return c.shutdown
}

func (c *Client) LastError() error {
	value := c.lastError.Load()

	if value == nil {
		return nil
	}

	return value.(error)
}

func (c *Client) run() (err error) {
	defer func() {
		c.options.Logger.Error().Err(err).
			Strs("server_urls", c.rawServerURLs).
			Msg("client_closed")
		c.channel.Close()
		c.cancel()
		c.lastError.Store(err)
		close(c.shutdown)
	}()

	serverURLManager_ := serverURLManager{
		Options: c.options,
	}

	err = serverURLManager_.LoadServerURLs(c.rawServerURLs)

	if err != nil {
		return
	}

	connectRetryCount := -1

	for {
		err = c.ctx.Err()

		if err != nil {
			return
		}

		connectRetryCount++

		if connectRetryCount >= 1 && c.options.WithoutConnectRetry {
			return
		}

		var serverURL *url.URL
		serverURL, err = serverURLManager_.GetNextServerURL(c.ctx, connectRetryCount)

		if err != nil {
			return
		}

		var connector Connector
		connector, err = GetConnector(serverURL.Scheme)

		if err != nil {
			return
		}

		var connection net.Conn
		connection, err = connector(c.ctx, c.getConnectTimeout(), serverURL)

		if err != nil {
			c.options.Logger.Error().Err(err).
				Str("server_url", serverURL.String()).
				Msg("client_connect_failed")
			continue
		}

		err = c.channel.Run(c.ctx, serverURL, connection)
		c.options.Logger.Error().Err(err).
			Str("server_url", serverURL.String()).
			Str("transport_id", c.channel.TransportID().String()).
			Msg("client_channel_run_failed")

		if _, ok := err.(*channel.NetworkError); ok {
			continue
		}

		if hangup, ok := err.(*channel.Hangup); ok && !hangup.IsPassive {
			return
		}

		if c.options.CloseOnChannelError {
			return
		}
	}
}

func (c *Client) getConnectTimeout() time.Duration {
	if connectTimeout := c.options.ConnectTimeout; connectTimeout >= 1 {
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

func (sum *serverURLManager) LoadServerURLs(rawServerURLs []string) error {
	for _, rawServerURL := range rawServerURLs {
		serverURL, err := url.Parse(rawServerURL)

		if err != nil {
			sum.Options.Logger.Warn().
				Err(err).
				Str("server_url", rawServerURL).
				Msg("client_invalid_server_url")
			continue
		}

		if _, err := GetConnector(serverURL.Scheme); err != nil {
			sum.Options.Logger.Warn().
				Err(err).
				Str("server_url", rawServerURL).
				Msg("client_invalid_server_url")
			continue
		}

		sum.serverURLs = append(sum.serverURLs, serverURL)
	}

	n := len(sum.serverURLs)

	if n == 0 {
		return ErrNoValidServerURL
	}

	rand.Shuffle(n, func(i, j int) {
		sum.serverURLs[i], sum.serverURLs[j] = sum.serverURLs[j], sum.serverURLs[i]
	})

	return nil
}

func (sum *serverURLManager) GetNextServerURL(ctx context.Context, connectRetryCount int) (*url.URL, error) {
	connectRetryOptions := &sum.Options.ConnectRetry

	if connectRetryOptions.MaxCount >= 1 && connectRetryCount > connectRetryOptions.MaxCount {
		return nil, ErrTooManyConnectRetries
	}

	if !connectRetryOptions.WithoutBackoff && connectRetryCount >= 1 {
		connectRetryBackoff := sum.connectRetryBackoff

		if connectRetryCount == 1 {
			connectRetryBackoff = connectRetryOptions.MinBackoff
		} else {
			connectRetryBackoff *= 2

			if connectRetryBackoff > connectRetryOptions.MaxBackoff {
				connectRetryBackoff = connectRetryOptions.MaxBackoff
			}
		}

		sum.connectRetryBackoff = connectRetryBackoff

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

	serverURL := sum.serverURLs[sum.nextServerURLIndex]
	sum.nextServerURLIndex = (sum.nextServerURLIndex + 1) % len(sum.serverURLs)
	return serverURL, nil
}
