package pbrpc

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

type Acceptor interface {
	Accept(context.Context, string, func(net.Conn)) error
}

type TCPAcceptor struct{}

func (TCPAcceptor) Accept(context_ context.Context, bindAddress string, connectionHandler func(net.Conn)) error {
	listener, e := net.Listen("tcp", bindAddress)

	if e != nil {
		return e
	}

	var wg sync.WaitGroup
	error_ := make(chan error, 1)

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
				connectionHandler(connection)
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

func (WebSocketAcceptor) Accept(context_ context.Context, bindAddress string, connectionHandler func(net.Conn)) error {
	server := http.Server{
		Addr: bindAddress,

		Handler: websocket.Handler(func(connection *websocket.Conn) {
			connectionHandler(connection)
		}),
	}

	error_ := make(chan error, 1)

	go func() {
		error_ <- server.ListenAndServe()
	}()

	var e error

	select {
	case e = <-error_:
		server.Shutdown(context.Background())
	case <-context_.Done():
		e = context_.Err()
		server.Shutdown(context.Background())
		<-error_
	}

	return e
}
