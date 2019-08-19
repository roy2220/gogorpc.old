package transport

import (
	"context"
	"fmt"
	"net"

	"golang.org/x/net/websocket"
)

type Connector func(ctx context.Context, serverAddress string) (connection net.Conn, err error)

func TCPConnector(ctx context.Context, serverAddress string) (net.Conn, error) {
	return new(net.Dialer).DialContext(ctx, "tcp", serverAddress)
}

func WebSocketConnector(ctx context.Context, serverAddress string) (net.Conn, error) {
	config, err := websocket.NewConfig(fmt.Sprintf("ws://%s/", serverAddress), "http://localhost/")

	if err != nil {
		return nil, err
	}

	config.Dialer = &net.Dialer{
		Cancel: ctx.Done(),
	}

	return websocket.DialConfig(config)
}
