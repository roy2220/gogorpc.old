package pbrpc

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/let-z-go/toolkit/logger"
)

func TestServer(t *testing.T) {
	sp := &ServerPolicy{
		Channel: &ServerChannelPolicy{
			ChannelPolicy: &ChannelPolicy{
				Timeout:            6 * time.Second,
				IncomingWindowSize: 20,
				OutgoingWindowSize: 200,
				Logger:             *(&logger.Logger{}).Initialize("pbrpctest-srv", logger.SeverityInfo, os.Stdout, os.Stderr),
			},
		},
	}

	cp1 := sp.Channel
	s := (&Server{}).Initialize(sp, "", "")

	cp2 := &ClientChannelPolicy{
		ChannelPolicy: &ChannelPolicy{
			Timeout:            5 * time.Second,
			IncomingWindowSize: 20,
			OutgoingWindowSize: 200,
			Logger:             *(&logger.Logger{}).Initialize("pbrpctest-cli", logger.SeverityInfo, os.Stdout, os.Stderr),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := (&ClientChannel{}).Initialize(cp2, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		go func() {
			// time.Sleep(time.Second)
			// c.impl.transport.connection.Close()
			time.Sleep(2 * time.Second)
			cancel()
		}()

		time.Sleep(time.Second / 2)
		c.Run(context.Background())
		wg.Done()
	}()

	s.Run(ctx)
	wg.Wait()

	if c.impl.timeout != cp1.Timeout {
		t.Errorf("%#v != %#v", c.impl.timeout, cp1.Timeout)
	}

	if c.impl.incomingWindowSize != cp2.IncomingWindowSize {
		t.Errorf("%#v != %#v", c.impl.incomingWindowSize, cp2.IncomingWindowSize)
	}

	if c.impl.outgoingWindowSize != cp1.IncomingWindowSize {
		t.Errorf("%#v != %#v", c.impl.incomingWindowSize, cp1.IncomingWindowSize)
	}
}

func TestServerGreeting(t *testing.T) {
	sp := &ServerPolicy{
		Channel: &ServerChannelPolicy{
			ChannelPolicy: &ChannelPolicy{
				Logger: *(&logger.Logger{}).Initialize("pbrpctest-srv", logger.SeverityInfo, os.Stdout, os.Stderr),
			},

			Handshaker: func(_ *ServerChannel, _ context.Context, handshake *[]byte) (bool, error) {
				n, e := strconv.Atoi(string(*handshake))

				if e != nil {
					t.Errorf("%v", e)
					return false, e
				}

				*handshake = []byte(strconv.Itoa(n + 1))
				return true, nil
			},
		},
	}

	s := (&Server{}).Initialize(sp, "", "")

	cp2 := &ClientChannelPolicy{
		ChannelPolicy: &ChannelPolicy{
			Logger: *(&logger.Logger{}).Initialize("pbrpctest-cli", logger.SeverityInfo, os.Stdout, os.Stderr),
		},

		Handshaker: func(_ *ClientChannel, context_ context.Context, greeter func(context.Context, *[]byte) error) error {
			handshake := []byte("99")
			e := greeter(context_, &handshake)

			if e == nil && string(handshake) != "100" {
				t.Errorf("%#v", handshake)
			}

			return e
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := (&ClientChannel{}).Initialize(cp2, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		go func() {
			time.Sleep(2 * time.Second)
			cancel()
		}()

		time.Sleep(time.Second / 2)
		c.Run(context.Background())
		wg.Done()
	}()

	s.Run(ctx)
	wg.Wait()
}
