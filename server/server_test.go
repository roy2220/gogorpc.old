package server

import (
	"context"
	"os"
	"testing"

	"github.com/rs/zerolog"

	"github.com/let-z-go/gogorpc/channel"
	"github.com/let-z-go/gogorpc/client"
)

func TestServerShutdown(t *testing.T) {
	opts := Options{
		ShutdownTimeout: -1,
		Channel: &channel.Options{
			Stream: &channel.StreamOptions{
				Transport: &channel.TransportOptions{
					Logger: &logger,
				},
			},
		},
	}
	opts.Channel.BuildMethod("", "").SetIncomingRPCHandler(func(rpc *channel.RPC) {
		rpc.Response = channel.NullMessage
	})
	s := new(Server).Init(&opts, "tcp://127.0.0.1:8000")
	go func() {
		c := new(client.Client).Init(&client.Options{Logger: &logger}, "tcp://127.0.0.1:8000")
		defer func() {
			c.Close()
			<-c.Shutdown()
			t.Log(c.LastError())
		}()
		c.InvokeRPC(&channel.RPC{
			Ctx:     context.Background(),
			Request: channel.NullMessage,
		}, channel.GetNullMessage)

		s.Close()
	}()
	go func() {
		t.Log(s.Run())
	}()
	s.WaitForShutdown()
}

var logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
