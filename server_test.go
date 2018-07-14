package pbrpc

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/let-z-go/toolkit/logger"
)

func TestServer(t *testing.T) {
	cp1 := &ChannelPolicy{
		Timeout:            6 * time.Second,
		IncomingWindowSize: 20,
		OutgoingWindowSize: 200,
		Logger:             *(&logger.Logger{}).Initialize("pbrpctest-srv", logger.SeverityInfo, os.Stdout, os.Stderr),
	}

	s := (&Server{}).Initialize("", nil, cp1, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		cp2 := &ChannelPolicy{
			Timeout:            5 * time.Second,
			IncomingWindowSize: 20,
			OutgoingWindowSize: 200,
			Logger:             *(&logger.Logger{}).Initialize("pbrpctest-cli", logger.SeverityInfo, os.Stdout, os.Stderr),
		}

		c := (&ClientChannel{}).Initialize(cp2, nil, nil)

		go func() {
			time.Sleep(time.Second)
			c.impl.transport.connection.Close()
			time.Sleep(time.Second)
			s.Stop(true)
		}()

		time.Sleep(time.Second / 2)
		c.Run()
		wg.Done()

		if c.impl.timeout != cp1.Timeout {
			t.Errorf("%#v != %#v", c.impl.timeout, cp1.Timeout)
		}

		if c.impl.incomingWindowSize != cp2.IncomingWindowSize {
			t.Errorf("%#v != %#v", c.impl.incomingWindowSize, cp2.IncomingWindowSize)
		}

		if c.impl.outgoingWindowSize != cp1.IncomingWindowSize {
			t.Errorf("%#v != %#v", c.impl.incomingWindowSize, cp1.IncomingWindowSize)
		}
	}()

	s.Run()
	wg.Wait()
}
