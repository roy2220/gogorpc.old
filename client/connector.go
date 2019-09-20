package client

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"time"
)

type Connector func(ctx context.Context, timeout time.Duration, serverURL *url.URL) (connection net.Conn, err error)

func RegisterConnector(schemeName string, connector Connector) error {
	if _, ok := connectors[schemeName]; ok {
		return &ConnectorExistsError{fmt.Sprintf("schemeName=%#v", schemeName)}
	}

	connectors[schemeName] = connector
	return nil
}

func MustRegisterConnector(schemeName string, connector Connector) {
	if err := RegisterConnector(schemeName, connector); err != nil {
		panic(err)
	}
}

func GetConnector(schemeName string) (Connector, error) {
	connector, ok := connectors[schemeName]

	if !ok {
		return nil, &ConnectorNotFoundError{fmt.Sprintf("schemeName=%#v", schemeName)}
	}

	return connector, nil
}

func MustGetConnector(schemeName string) Connector {
	connector, err := GetConnector(schemeName)

	if err != nil {
		panic(err)
	}

	return connector
}

type ConnectorExistsError struct {
	context string
}

func (self ConnectorExistsError) Error() string {
	message := "gogorpc/client: connector exists"

	if self.context != "" {
		message += ": " + self.context
	}

	return message
}

type ConnectorNotFoundError struct {
	context string
}

func (self ConnectorNotFoundError) Error() string {
	message := "gogorpc/client: connector not found"

	if self.context != "" {
		message += ": " + self.context
	}

	return message
}

var connectors = map[string]Connector{}

func tcpConnector(ctx context.Context, timeout time.Duration, serverURL *url.URL) (net.Conn, error) {
	return (&net.Dialer{
		Timeout: timeout,
	}).DialContext(ctx, "tcp", serverURL.Host)
}

func init() {
	MustRegisterConnector("tcp", tcpConnector)
}
