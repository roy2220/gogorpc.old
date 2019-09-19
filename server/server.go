package server

import (
	"context"
	"errors"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/let-z-go/pbrpc/channel"
)

type Server struct {
	options  *Options
	rawURL   string
	ctx      context.Context
	cancel   context.CancelFunc
	activity activity
}

func (self *Server) Init(options *Options, rawURL string) *Server {
	self.options = options.Normalize()
	self.rawURL = rawURL
	self.ctx, self.cancel = context.WithCancel(context.Background())
	self.activity.Init(self.ctx, self.options.ShutdownTimeout)
	return self
}

func (self *Server) Close() {
	self.cancel()
	self.activity.Close()
}

func (self *Server) Run() (err error) {
	if self.activity.IsClosed() {
		self.options.Logger.Error().
			Str("server_url", self.rawURL).
			Msg("server_already_closed")
		return ErrClosed
	}

	atomic.AddInt32(&self.activity.Counter, 1)

	defer func() {
		self.options.Logger.Info().
			Str("server_url", self.rawURL).
			Msg("server_closed")

		if self.activity.IsClosed() {
			err = ErrClosed
		} else {
			self.cancel()
		}

		atomic.AddInt32(&self.activity.Counter, -1)
	}()

	if self.activity.IsClosed() {
		return
	}

	url_, err := url.Parse(self.rawURL)

	if err != nil {
		self.options.Logger.Error().
			Err(err).
			Str("server_url", self.rawURL).
			Msg("server_invalid_url")
		return err
	}

	acceptor, err := GetAcceptor(url_.Scheme)

	if err != nil {
		self.options.Logger.Error().
			Err(err).
			Str("server_url", self.rawURL).
			Msg("server_bad_scheme")
		return err
	}

	err = acceptor(self.ctx, url_, &self.activity.Counter, func(connection net.Conn) {
		channel_ := new(channel.Channel).Init(self.options.Channel)
		defer channel_.Close()

		if err := channel_.Accept(self.activity.Ctx, connection); err != nil {
			self.options.Logger.Warn().Err(err).
				Str("server_url", self.rawURL).
				Str("transport_id", channel_.GetTransportID().String()).
				Msg("server_channel_accept_failed")
			return
		}

		err := channel_.Process(self.activity.Ctx)
		self.options.Logger.Warn().Err(err).
			Str("server_url", self.rawURL).
			Str("transport_id", channel_.GetTransportID().String()).
			Msg("server_channel_process_failed")
	})

	self.options.Logger.Error().Err(err).
		Str("server_url", self.rawURL).
		Msg("server_accept_failed")
	return err
}

func (self *Server) WaitForShutdown() bool {
	return self.activity.WaitFor()
}

var ErrClosed = errors.New("pbrpc/server: closed")

type activity struct {
	Ctx     context.Context
	Counter int32

	isClosed int32
}

func (self *activity) Init(ctx context.Context, overtime time.Duration) {
	if overtime < 0 {
		self.Ctx = context.Background()
	} else if overtime == 0 {
		self.Ctx = ctx
	} else {
		var cancel context.CancelFunc
		self.Ctx, cancel = context.WithCancel(context.Background())

		go func() {
			select {
			case <-ctx.Done():
				time.Sleep(overtime)
				cancel()
			}
		}()
	}

	self.Counter = 1
}

func (self *activity) Close() {
	if atomic.CompareAndSwapInt32(&self.isClosed, 0, 1) {
		atomic.AddInt32(&self.Counter, -1)
	}
}

func (self *activity) WaitFor() bool {
	if self.Ctx.Err() != nil {
		return false
	}

	ticker := time.NewTicker(activityPollInterval)
	defer ticker.Stop()

	for {
		if atomic.LoadInt32(&self.Counter) == 0 {
			return true
		}

		select {
		case <-self.Ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func (self *activity) IsClosed() bool {
	return atomic.LoadInt32(&self.isClosed) == 1
}

const activityPollInterval = 500 * time.Millisecond
