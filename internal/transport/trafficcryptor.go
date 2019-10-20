package transport

type TrafficCrypter interface {
	TrafficDecrypter
	TrafficEncrypter
}

type TrafficDecrypter interface {
	DecryptTraffic(traffic []byte)
}

type TrafficEncrypter interface {
	EncryptTraffic(traffic []byte)
}

type DummyTrafficCrypter struct {
	DummyTrafficDecrypter
	DummyTrafficEncrypter
}

var _ = TrafficCrypter(DummyTrafficCrypter{})

type DummyTrafficDecrypter struct{}

var _ = TrafficDecrypter(DummyTrafficDecrypter{})

func (DummyTrafficDecrypter) DecryptTraffic([]byte) {}

type DummyTrafficEncrypter struct{}

var _ = TrafficEncrypter(DummyTrafficEncrypter{})

func (DummyTrafficEncrypter) EncryptTraffic([]byte) {}
