package clientserver

import (
	"net"
	"net/url"
)

func NormalizeServerURL(serverURL *url.URL) {
	hostname := serverURL.Hostname()
	port := serverURL.Port()

	if hostname == "" {
		hostname = "127.0.0.1"
	}

	if port == "" {
		port += "8888"
	}

	serverURL.Host = net.JoinHostPort(hostname, port)
}
