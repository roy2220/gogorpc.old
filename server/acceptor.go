package server

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync/atomic"
	"time"
)

type Acceptor func(ctx context.Context, url_ *url.URL, activityCounter *int32, connectionHandler ConnectionHandler) error
type ConnectionHandler func(connection net.Conn)

func RegisterAcceptor(schemeName string, acceptor Acceptor) error {
	if _, ok := acceptors[schemeName]; ok {
		return &AcceptorExistsError{fmt.Sprintf("schemeName=%#v", schemeName)}
	}

	acceptors[schemeName] = acceptor
	return nil
}

func MustRegisterAcceptor(schemeName string, acceptor Acceptor) {
	if err := RegisterAcceptor(schemeName, acceptor); err != nil {
		panic(err)
	}
}

func GetAcceptor(schemeName string) (Acceptor, error) {
	acceptor, ok := acceptors[schemeName]

	if !ok {
		return nil, &AcceptorNotFoundError{fmt.Sprintf("schemeName=%#v", schemeName)}
	}

	return acceptor, nil
}

func MustGetAcceptor(schemeName string) Acceptor {
	acceptor, err := GetAcceptor(schemeName)

	if err != nil {
		panic(err)
	}

	return acceptor
}

type AcceptorExistsError struct {
	context string
}

func (self AcceptorExistsError) Error() string {
	message := "gogorpc/server: acceptor exists"

	if self.context != "" {
		message += ": " + self.context
	}

	return message
}

type AcceptorNotFoundError struct {
	context string
}

func (self AcceptorNotFoundError) Error() string {
	message := "gogorpc/server: acceptor not found"

	if self.context != "" {
		message += ": " + self.context
	}

	return message
}

var acceptors = map[string]Acceptor{}

func tcpAcceptor(ctx context.Context, url_ *url.URL, activityCounter *int32, connectionHandler ConnectionHandler) error {
	listener, err := net.Listen("tcp", url_.Host)

	if err != nil {
		return err
	}

	err2 := make(chan error, 1)

	go func() {
		retryBackoff := time.Duration(0)

		for {
			connection, err := listener.Accept()

			if err != nil {
				if error_, ok := err.(net.Error); ok && error_.Temporary() {
					const minRetryBackoff = 5 * time.Millisecond
					const maxRetryBackoff = 1 * time.Second

					if retryBackoff == 0 {
						retryBackoff = minRetryBackoff
					} else {
						retryBackoff *= 2

						if retryBackoff > maxRetryBackoff {
							retryBackoff = maxRetryBackoff
						}
					}

					time.Sleep(retryBackoff)
					continue
				} else {
					err2 <- err
					return
				}
			}

			retryBackoff = 0
			atomic.AddInt32(activityCounter, 1)

			go func() {
				connectionHandler(connection)
				atomic.AddInt32(activityCounter, -1)
			}()
		}
	}()

	select {
	case <-ctx.Done():
		err = ctx.Err()
		listener.Close()
		<-err2
	case err = <-err2:
		listener.Close()
	}

	return err
}

func init() {
	MustRegisterAcceptor("tcp", tcpAcceptor)
}
