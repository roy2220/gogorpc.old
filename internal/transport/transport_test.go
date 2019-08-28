package transport

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/let-z-go/toolkit/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	"github.com/let-z-go/pbrpc/internal/protocol"
)

func TestOptions(t *testing.T) {
	type PureOptions struct {
		Logger                *zerolog.Logger
		Connector             Connector
		HandshakeTimeout      time.Duration
		MinInputBufferSize    int
		MaxInputBufferSize    int
		MaxIncomingPacketSize int
		MaxOutgoingPacketSize int
	}
	makePureOptions := func(opt *Options) PureOptions {
		return PureOptions{
			HandshakeTimeout:      opt.HandshakeTimeout,
			MinInputBufferSize:    opt.MinInputBufferSize,
			MaxInputBufferSize:    opt.MaxInputBufferSize,
			MaxIncomingPacketSize: opt.MaxIncomingPacketSize,
			MaxOutgoingPacketSize: opt.MaxOutgoingPacketSize,
		}
	}
	{
		opt1 := Options{}
		opt1.Normalize()
		opt2 := Options{
			HandshakeTimeout:      defaultHandshakeTimeout,
			MinInputBufferSize:    defaultMinInputBufferSize,
			MaxInputBufferSize:    defaultMaxInputBufferSize,
			MaxIncomingPacketSize: defaultMaxPacketSize,
			MaxOutgoingPacketSize: defaultMaxPacketSize,
		}
		assert.Equal(t, makePureOptions(&opt2), makePureOptions(&opt1))
	}
	{
		opt1 := Options{
			HandshakeTimeout:      -1,
			MinInputBufferSize:    -1,
			MaxInputBufferSize:    -1,
			MaxIncomingPacketSize: -1,
			MaxOutgoingPacketSize: -1,
		}
		opt1.Normalize()
		opt2 := Options{
			HandshakeTimeout:      minHandshakeTimeout,
			MinInputBufferSize:    minInputBufferSize,
			MaxInputBufferSize:    minInputBufferSize,
			MaxIncomingPacketSize: minInputBufferSize,
			MaxOutgoingPacketSize: minMaxPacketSize,
		}
		assert.Equal(t, makePureOptions(&opt2), makePureOptions(&opt1))
	}
	{
		opt1 := Options{
			HandshakeTimeout:      math.MaxInt64,
			MinInputBufferSize:    math.MaxInt32,
			MaxInputBufferSize:    math.MaxInt32,
			MaxIncomingPacketSize: math.MaxInt32,
			MaxOutgoingPacketSize: math.MaxInt32,
		}
		opt1.Normalize()
		opt2 := Options{
			HandshakeTimeout:      maxHandshakeTimeout,
			MinInputBufferSize:    maxInputBufferSize,
			MaxInputBufferSize:    maxInputBufferSize,
			MaxIncomingPacketSize: maxMaxPacketSize,
			MaxOutgoingPacketSize: maxMaxPacketSize,
		}
		assert.Equal(t, makePureOptions(&opt2), makePureOptions(&opt1))
	}
}

func TestHandshake1(t *testing.T) {
	logger := zerolog.New(os.Stdout)
	testSetup(
		t,
		func(ctx context.Context, sa string) {
			tp := new(Transport).Init(&Options{Logger: &logger, Connector: TCPConnector})
			defer tp.Close()
			ok, err := tp.Connect(ctx, sa, uuid.GenerateUUID4Fast(), testHandshaker{
				CbSizeHandshake: func() int {
					return len("hello")
				},
				CbEmitHandshake: func(buf []byte) error {
					copy(buf, "hello")
					return nil
				},
				CbHandleHandshake: func(ctx context.Context, rh []byte) (bool, error) {
					if string(rh) != "nick to meet you" {
						return false, nil
					}
					return true, nil
				},
			})
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.True(t, ok)
		},
		func(ctx context.Context, conn net.Conn) {
			tp := new(Transport).Init(&Options{Logger: &logger})
			defer tp.Close()
			ok, err := tp.Accept(ctx, conn, testHandshaker{
				CbHandleHandshake: func(ctx context.Context, rh []byte) (bool, error) {
					if string(rh) != "hello" {
						return false, nil
					}
					return true, nil
				},
				CbSizeHandshake: func() int {
					return len("nick to meet you")
				},
				CbEmitHandshake: func(buf []byte) error {
					copy(buf, "nick to meet you")
					return nil
				},
			})
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.True(t, ok)
		},
	)
}

func TestHandshake2(t *testing.T) {
	testSetup(
		t,
		func(ctx context.Context, sa string) {
			tp := new(Transport).Init(&Options{Connector: TCPConnector, HandshakeTimeout: -1})
			defer tp.Close()
			ok, err := tp.Connect(ctx, sa, uuid.GenerateUUID4Fast(), testHandshaker{
				CbSizeHandshake: func() int {
					return 0
				},
				CbEmitHandshake: func(buf []byte) error {
					return nil
				},
				CbHandleHandshake: func(ctx context.Context, rh []byte) (bool, error) {
					return true, nil
				},
			})
			if !assert.EqualError(t, err, "pbrpc/transport: network: context deadline exceeded") {
				t.FailNow()
			}
			assert.False(t, ok)
		},
		func(ctx context.Context, conn net.Conn) {
			tp := new(Transport).Init(&Options{HandshakeTimeout: -1})
			defer tp.Close()
			ok, err := tp.Accept(ctx, conn, testHandshaker{
				CbHandleHandshake: func(ctx context.Context, rh []byte) (bool, error) {
					<-ctx.Done()
					<-time.After(10 * time.Millisecond)
					return true, ctx.Err()
				},
				CbSizeHandshake: func() int {
					return 0
				},
				CbEmitHandshake: func(buf []byte) error {
					return nil
				},
			})
			if !assert.EqualError(t, err, "context deadline exceeded") {
				t.FailNow()
			}
			assert.False(t, ok)
		},
	)
}

func TestHandshake3(t *testing.T) {
	testSetup(
		t,
		func(ctx context.Context, sa string) {
			tp := new(Transport).Init(&Options{Connector: TCPConnector, HandshakeTimeout: -1})
			defer tp.Close()
			ok, err := tp.Connect(ctx, sa, uuid.GenerateUUID4Fast(), testHandshaker{
				CbSizeHandshake: func() int {
					return 0
				},
				CbEmitHandshake: func(buf []byte) error {
					return nil
				},
				CbHandleHandshake: func(ctx context.Context, rh []byte) (bool, error) {
					<-ctx.Done()
					<-time.After(10 * time.Millisecond)
					return false, ctx.Err()
				},
			})
			if !assert.EqualError(t, err, "context deadline exceeded") {
				t.FailNow()
			}
			assert.False(t, ok)
		},
		func(ctx context.Context, conn net.Conn) {
			tp := new(Transport).Init(&Options{HandshakeTimeout: -1})
			defer tp.Close()
			ok, err := tp.Accept(ctx, conn, testHandshaker{
				CbHandleHandshake: func(ctx context.Context, rh []byte) (bool, error) {
					return false, nil
				},
				CbSizeHandshake: func() int {
					return 0
				},
				CbEmitHandshake: func(buf []byte) error {
					return nil
				},
			})
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.False(t, ok)
		},
	)
}

func TestSendAndReceivePackets(t *testing.T) {
	const N = 1000
	makeMessageType := func(i int) protocol.MessageType {
		switch i % 4 {
		case 0:
			return protocol.MESSAGE_RESPONSE
		case 1:
			return protocol.MESSAGE_REQUEST
		case 2:
			return protocol.MESSAGE_HANGUP
		default:
			return protocol.MESSAGE_KEEPALIVE
		}
	}
	opts1 := Options{Connector: TCPConnector}
	opts2 := Options{}
	cb1 := func(ctx context.Context, tp *Transport) {
		m := 1
		for i := 0; i < N; i++ {
			msg := fmt.Sprintf("this packet %d", i)
			err := tp.Write(&Packet{
				Header: protocol.PacketHeader{
					MessageType: makeMessageType(i),
				},
				PayloadSize: len(msg),
			}, func(buf []byte) error {
				copy(buf, msg)
				return nil
			})
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			if i%m == 0 {
				err = tp.Flush(ctx, 0)
				if !assert.NoError(t, err, i) {
					t.FailNow()
				}
				m = (m+1)%13 + 1
			}
		}
		err := tp.Flush(ctx, 0)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		tp.Close()
	}
	cb2 := func(ctx context.Context, tp *Transport) {
		pk := Packet{}
		i := 0
		for {
			err := tp.Peek(ctx, 0, &pk)
			if err != nil {
				if !assert.EqualError(t, err, "pbrpc/transport: network: EOF", i) {
					t.FailNow()
				}
				if !assert.Equal(t, i, N) {
					t.FailNow()
				}
				break
			}
			if !assert.Equal(t, makeMessageType(i), pk.Header.MessageType, i) {
				t.FailNow()
			}
			if !assert.Equal(t, fmt.Sprintf("this packet %d", i), string(pk.Payload)) {
				t.FailNow()
			}
			i++
			for {
				ok, err := tp.PeekNext(&pk)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
				if !ok {
					break
				}
				if !assert.Equal(t, makeMessageType(i), pk.Header.MessageType, i) {
					t.FailNow()
				}
				if !assert.Equal(t, fmt.Sprintf("this packet %d", i), string(pk.Payload)) {
					t.FailNow()
				}
				i++
			}
		}
	}
	testSetup2(t, &opts1, &opts2, cb1, cb2)
	testSetup2(t, &opts1, &opts2, cb2, cb1)
}

func TestSendBadPacket1(t *testing.T) {
	opts1 := Options{Connector: TCPConnector, MaxOutgoingPacketSize: -1}
	opts2 := Options{MaxOutgoingPacketSize: -1}
	cb1 := func(ctx context.Context, tp *Transport) {
		err := tp.Write(&Packet{
			Header: protocol.PacketHeader{
				MessageType: protocol.MESSAGE_REQUEST,
			},
			PayloadSize: minMaxPacketSize,
		}, func([]byte) error { return nil })
		if !assert.EqualError(t, err, ErrPacketTooLarge.Error()) {
			t.FailNow()
		}
	}
	cb2 := func(ctx context.Context, tp *Transport) {
	}
	testSetup2(t, &opts1, &opts2, cb1, cb2)
	testSetup2(t, &opts1, &opts2, cb2, cb1)
}

func TestSendBadPacket2(t *testing.T) {
	errTest := errors.New("my test")
	opts1 := Options{Connector: TCPConnector, MaxOutgoingPacketSize: -1}
	opts2 := Options{MaxOutgoingPacketSize: -1}
	cb1 := func(ctx context.Context, tp *Transport) {
		err := tp.Write(&Packet{
			Header: protocol.PacketHeader{
				MessageType: protocol.MESSAGE_REQUEST,
			},
			PayloadSize: 0,
		}, func([]byte) error { return errTest })
		if !assert.EqualError(t, err, errTest.Error()) {
			t.FailNow()
		}
	}
	cb2 := func(ctx context.Context, tp *Transport) {
	}
	testSetup2(t, &opts1, &opts2, cb1, cb2)
	testSetup2(t, &opts1, &opts2, cb2, cb1)
}

type testHandshaker struct {
	CbHandleHandshake func(context.Context, []byte) (bool, error)
	CbSizeHandshake   func() int
	CbEmitHandshake   func([]byte) error
}

func (s testHandshaker) HandleHandshake(ctx context.Context, rh []byte) (bool, error) {
	return s.CbHandleHandshake(ctx, rh)
}

func (s testHandshaker) SizeHandshake() int {
	return s.CbSizeHandshake()
}

func (s testHandshaker) EmitHandshake(buf []byte) error {
	return s.CbEmitHandshake(buf)
}

func testSetup(
	t *testing.T,
	cb1 func(ctx context.Context, sa string),
	cb2 func(ctx context.Context, conn net.Conn),
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer l.Close()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		cb1(ctx, l.Addr().String())
	}()
	defer wg.Wait()
	c, err := l.Accept()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	cb2(ctx, c)
}

func testSetup2(
	t *testing.T,
	opts1 *Options,
	opts2 *Options,
	cb1 func(ctx context.Context, tp *Transport),
	cb2 func(ctx context.Context, tp *Transport),
) {
	testSetup(
		t,
		func(ctx context.Context, sa string) {
			tp := new(Transport).Init(opts1)
			defer tp.Close()
			ok, err := tp.Connect(ctx, sa, uuid.GenerateUUID4Fast(), testHandshaker{
				CbSizeHandshake: func() int {
					return 0
				},
				CbEmitHandshake: func(buf []byte) error {
					return nil
				},
				CbHandleHandshake: func(ctx context.Context, rh []byte) (bool, error) {
					return true, nil
				},
			})
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			if !assert.True(t, ok) {
				t.FailNow()
			}
			cb1(ctx, tp)
		},
		func(ctx context.Context, conn net.Conn) {
			tp := new(Transport).Init(opts2)
			defer tp.Close()
			ok, err := tp.Accept(ctx, conn, testHandshaker{
				CbHandleHandshake: func(ctx context.Context, rh []byte) (bool, error) {
					return true, nil
				},
				CbSizeHandshake: func() int {
					return 0
				},
				CbEmitHandshake: func(buf []byte) error {
					return nil
				},
			})
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			if !assert.True(t, ok) {
				t.FailNow()
			}
			cb2(ctx, tp)
		},
	)
}
