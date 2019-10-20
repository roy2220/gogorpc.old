package channel

import (
	"context"
	"net/url"
)

type Extension interface {
	Listener

	NewUserData() interface{}
	NewHandshaker() Handshaker
	NewTrafficCrypter() TrafficCrypter
	NewKeepaliver() Keepaliver
}

type Listener interface {
	OnInitialized()
	OnEstablishing(serverURL *url.URL)
	OnReestablishing(serverURL *url.URL)
	OnEstablished()
	OnBroken(err error)
	OnClosed()
}

type Keepaliver interface {
	NewKeepalive() (keepalive Message)
	HandleKeepalive(ctx context.Context, keepalive Message) (err error)
	EmitKeepalive() (keepalive Message, err error)
}

type ExtensionFactory func(channel RestrictedChannel, channelIsServerSide bool) (extension Extension)

type DummyExtension struct {
	DummyListener
}

var _ = Extension(DummyExtension{})

func (DummyExtension) NewUserData() interface{}          { return nil }
func (DummyExtension) NewHandshaker() Handshaker         { return DummyHandshaker{} }
func (DummyExtension) NewTrafficCrypter() TrafficCrypter { return DummyTrafficCrypter{} }
func (DummyExtension) NewKeepaliver() Keepaliver         { return DummyKeepaliver{} }

type DummyListener struct{}

var _ = Listener(DummyListener{})

func (DummyListener) OnInitialized()            {}
func (DummyListener) OnEstablishing(*url.URL)   {}
func (DummyListener) OnReestablishing(*url.URL) {}
func (DummyListener) OnEstablished()            {}
func (DummyListener) OnBroken(error)            {}
func (DummyListener) OnClosed()                 {}

type DummyKeepaliver struct{}

var _ = Keepaliver(DummyKeepaliver{})

func (DummyKeepaliver) NewKeepalive() Message                          { return NullMessage }
func (DummyKeepaliver) HandleKeepalive(context.Context, Message) error { return nil }
func (DummyKeepaliver) EmitKeepalive() (Message, error)                { return NullMessage, nil }

func DummyExtensionFactory(RestrictedChannel, bool) Extension {
	return DummyExtension{}
}

var _ = ExtensionFactory(DummyExtensionFactory)
