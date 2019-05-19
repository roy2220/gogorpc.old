package pbrpc

import (
	"context"
	"fmt"
	"net"

	"golang.org/x/net/websocket"
)

type Connector interface {
	Connect(context_ context.Context, serverAddres string) (connection net.Conn, e error)
}

type TCPConnector struct{}

func (TCPConnector) Connect(context_ context.Context, serverAddress string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(context_, "tcp", serverAddress)
}

type WebSocketConnector struct{}

func (WebSocketConnector) Connect(context_ context.Context, serverAddress string) (net.Conn, error) {
	config, e := websocket.NewConfig(fmt.Sprintf("ws://%s/", serverAddress), "http://localhost/")

	if e != nil {
		return nil, e
	}

	config.Dialer = &net.Dialer{
		Cancel: context_.Done(),
	}

	return websocket.DialConfig(config)
}
