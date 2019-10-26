package server

import (
	"context"
	"errors"
	"net"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/let-z-go/gogorpc/channel"
)

type Server struct {
	options  *Options
	rawURL   string
	ctx      context.Context
	cancel   context.CancelFunc
	activity activity
}

func (s *Server) Init(options *Options, rawURL string) *Server {
	s.options = options.Normalize()
	s.rawURL = rawURL
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.activity.Init(s.ctx, s.options.ShutdownTimeout)
	return s
}

func (s *Server) Close() {
	s.cancel()
	s.activity.Close()
}

func (s *Server) Run() (err error) {
	if s.activity.IsClosed() {
		s.options.Logger.Error().
			Str("server_url", s.rawURL).
			Msg("server_already_closed")
		return ErrClosed
	}

	atomic.AddInt32(&s.activity.Counter, 1)

	defer func() {
		if err == ErrClosed {
			err = nil
		} else {
			s.Close()
		}

		s.options.Logger.Error().Err(err).
			Str("server_url", s.rawURL).
			Msg("server_closed")
		atomic.AddInt32(&s.activity.Counter, -1)
	}()

	if s.activity.IsClosed() {
		return ErrClosed
	}

	var url_ *url.URL
	url_, err = url.Parse(s.rawURL)

	if err != nil {
		s.options.Logger.Error().Err(err).
			Str("server_url", s.rawURL).
			Msg("server_invalid_url")
		return
	}

	var acceptor Acceptor
	acceptor, err = GetAcceptor(url_.Scheme)

	if err != nil {
		return
	}

	for i, hook := range s.options.Hooks {
		if hook.BeforeRun == nil {
			continue
		}

		err = hook.BeforeRun(s.ctx, url_)

		if err != nil {
			for i--; i >= 0; i-- {
				hook = s.options.Hooks[i]

				if hook.AfterRun == nil {
					continue
				}

				hook.AfterRun(url_)
			}

			return
		}
	}

	err = acceptor(s.ctx, url_, &s.activity.Counter, func(connection net.Conn) {
		channel_ := new(channel.Channel).Init(s.options.Channel, true)
		defer channel_.Close()
		err := channel_.Run(s.activity.Ctx, url_, connection)
		s.options.Logger.Warn().Err(err).
			Str("server_url", s.rawURL).
			Str("transport_id", channel_.TransportID().String()).
			Msg("server_channel_run_failed")
	})

	s.options.Logger.Error().Err(err).
		Str("server_url", s.rawURL).
		Msg("server_accept_failed")

	for _, hook := range s.options.Hooks {
		if hook.AfterRun == nil {
			continue
		}

		hook.AfterRun(url_)
	}

	return
}

func (s *Server) WaitForShutdown() bool {
	return s.activity.WaitFor()
}

var ErrClosed = errors.New("gogorpc/server: closed")

type activity struct {
	Ctx     context.Context
	Counter int32

	isClosed int32
}

func (a *activity) Init(ctx context.Context, overtime time.Duration) {
	if overtime < 0 {
		a.Ctx = context.Background()
	} else if overtime == 0 {
		a.Ctx = ctx
	} else {
		var cancel context.CancelFunc
		a.Ctx, cancel = context.WithCancel(context.Background())

		go func() {
			select {
			case <-ctx.Done():
				time.Sleep(overtime)
				cancel()
			}
		}()
	}

	a.Counter = 1
}

func (a *activity) Close() {
	if atomic.CompareAndSwapInt32(&a.isClosed, 0, 1) {
		atomic.AddInt32(&a.Counter, -1)
	}
}

func (a *activity) WaitFor() bool {
	if a.Ctx.Err() != nil {
		return false
	}

	ticker := time.NewTicker(activityPollInterval)
	defer ticker.Stop()

	for {
		if atomic.LoadInt32(&a.Counter) == 0 {
			return true
		}

		select {
		case <-a.Ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func (a *activity) IsClosed() bool {
	return atomic.LoadInt32(&a.isClosed) == 1
}

const activityPollInterval = 500 * time.Millisecond
