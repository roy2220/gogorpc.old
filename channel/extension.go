package channel

import (
	"context"
	"net/url"
)

type Extension interface {
	Listener
	Handshaker
	Keepaliver
	MessageFilter
}

type Listener interface {
	OnEstablishing(channel *Channel, serverURL *url.URL)
	OnReestablishing(channel *Channel, serverURL *url.URL)
	OnEstablished(channel *Channel)
	OnBroken(channel *Channel, err error)
	OnClosed(channel *Channel)
}

type Keepaliver interface {
	NewKeepalive() (keepalive Message)
	HandleKeepalive(ctx context.Context, keepalive Message) (err error)
	EmitKeepalive() (keepalive Message, err error)
}

type DummyExtension struct {
	DummyListener
	DummyHandshaker
	DummyKeepaliver
	DummyMessageFilter
}

var _ = Extension(DummyExtension{})

type DummyListener struct{}

var _ = Listener(DummyListener{})

func (DummyListener) OnEstablishing(*Channel, *url.URL)   {}
func (DummyListener) OnReestablishing(*Channel, *url.URL) {}
func (DummyListener) OnEstablished(*Channel)              {}
func (DummyListener) OnBroken(*Channel, error)            {}
func (DummyListener) OnClosed(*Channel)                   {}

type DummyKeepaliver struct{}

var _ = Keepaliver(DummyKeepaliver{})

func (DummyKeepaliver) NewKeepalive() Message                          { return NullMessage }
func (DummyKeepaliver) HandleKeepalive(context.Context, Message) error { return nil }
func (DummyKeepaliver) EmitKeepalive() (Message, error)                { return NullMessage, nil }
