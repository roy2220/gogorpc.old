package pbrpc

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/let-z-go/toolkit/utils"
	"golang.org/x/net/websocket"
)

type Acceptor interface {
	Accept(context.Context, string, time.Duration, func(context.Context, net.Conn)) error
}

type TCPAcceptor struct{}

func (TCPAcceptor) Accept(context_ context.Context, bindAddress string, gracefulShutdownTimeout time.Duration, connectionHandler func(context.Context, net.Conn)) error {
	listener, e := net.Listen("tcp", bindAddress)

	if e != nil {
		return e
	}

	error_ := make(chan error, 1)
	var wg sync.WaitGroup
	context2 := utils.DelayContext(context_, gracefulShutdownTimeout)

	go func() {
		retryDelay := time.Duration(0)

		for {
			connection, e := listener.Accept()

			if e != nil {
				if e2, ok := e.(net.Error); ok && e2.Temporary() {
					const minRetryDelay = 5 * time.Millisecond
					const maxRetryDelay = 1 * time.Second

					if retryDelay == 0 {
						retryDelay = minRetryDelay
					} else {
						retryDelay *= 2

						if retryDelay > maxRetryDelay {
							retryDelay = maxRetryDelay
						}
					}

					time.Sleep(retryDelay)
					continue
				}

				error_ <- e
				return
			}

			wg.Add(1)

			go func() {
				connectionHandler(context2, connection)
				wg.Done()
			}()

			retryDelay = 0
		}
	}()

	select {
	case e = <-error_:
		listener.Close()
	case <-context_.Done():
		e = context_.Err()
		listener.Close()
		<-error_
	}

	wg.Wait()
	return e
}

type WebSocketAcceptor struct{}

func (WebSocketAcceptor) Accept(context_ context.Context, bindAddress string, gracefulShutdownTimeout time.Duration, connectionHandler func(context.Context, net.Conn)) error {
	var wg sync.WaitGroup
	context2 := utils.DelayContext(context_, gracefulShutdownTimeout)

	server := http.Server{
		Addr: bindAddress,

		ConnState: func(_ net.Conn, connState http.ConnState) {
			switch connState {
			case http.StateNew:
				wg.Add(1)
			case http.StateClosed:
				wg.Done()
			}
		},

		Handler: websocket.Server{
			Handshake: func(config *websocket.Config, request *http.Request) error {
				origin, e := websocket.Origin(config, request)

				if e != nil || origin == nil {
					origin = &url.URL{Opaque: request.RemoteAddr}
				}

				config.Origin = origin
				return nil
			},

			Handler: func(connection *websocket.Conn) {
				connection.PayloadType = websocket.BinaryFrame
				connection.MaxPayloadBytes = maxWebSocketFramePayloadSize
				connectionHandler(context2, connection)
				wg.Done()
			},
		},
	}

	error_ := make(chan error, 1)

	go func() {
		error_ <- server.ListenAndServe()
	}()

	var e error

	select {
	case e = <-error_:
		server.Close()
	case <-context_.Done():
		e = context_.Err()
		server.Close()
		<-error_
	}

	wg.Wait()
	return e
}

const maxWebSocketFramePayloadSize = 1 << 16
