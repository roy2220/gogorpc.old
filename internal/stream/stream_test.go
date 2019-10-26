package stream

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/let-z-go/toolkit/uuid"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	"github.com/let-z-go/gogorpc/internal/proto"
	"github.com/let-z-go/gogorpc/internal/transport"
)

func TestOptions(t *testing.T) {
	type PureOptions struct {
		ActiveHangupTimeout       time.Duration
		IncomingKeepaliveInterval time.Duration
		OutgoingKeepaliveInterval time.Duration
		IncomingConcurrencyLimit  int
		OutgoingConcurrencyLimit  int
	}
	makePureOptions := func(opts *Options) PureOptions {
		return PureOptions{
			ActiveHangupTimeout:       opts.ActiveHangupTimeout,
			IncomingKeepaliveInterval: opts.IncomingKeepaliveInterval,
			OutgoingKeepaliveInterval: opts.OutgoingKeepaliveInterval,
			IncomingConcurrencyLimit:  opts.IncomingConcurrencyLimit,
			OutgoingConcurrencyLimit:  opts.OutgoingConcurrencyLimit,
		}
	}
	{
		opts1 := Options{
			ActiveHangupTimeout:       -1,
			IncomingKeepaliveInterval: -1,
			OutgoingKeepaliveInterval: -1,
			IncomingConcurrencyLimit:  -1,
			OutgoingConcurrencyLimit:  -1,
		}
		opts1.Normalize()
		opts2 := Options{
			ActiveHangupTimeout:       minActiveHangupTimeout,
			IncomingKeepaliveInterval: minKeepaliveInterval,
			OutgoingKeepaliveInterval: minKeepaliveInterval,
			IncomingConcurrencyLimit:  minConcurrencyLimit,
			OutgoingConcurrencyLimit:  minConcurrencyLimit,
		}
		assert.Equal(t, makePureOptions(&opts2), makePureOptions(&opts1))
	}
	{
		opts1 := Options{
			ActiveHangupTimeout:       math.MaxInt64,
			IncomingKeepaliveInterval: math.MaxInt64,
			OutgoingKeepaliveInterval: math.MaxInt64,
			IncomingConcurrencyLimit:  math.MaxInt32,
			OutgoingConcurrencyLimit:  math.MaxInt32,
		}
		opts1.Normalize()
		opts2 := Options{
			ActiveHangupTimeout:       maxActiveHangupTimeout,
			IncomingKeepaliveInterval: maxKeepaliveInterval,
			OutgoingKeepaliveInterval: maxKeepaliveInterval,
			IncomingConcurrencyLimit:  maxConcurrencyLimit,
			OutgoingConcurrencyLimit:  maxConcurrencyLimit,
		}
		assert.Equal(t, makePureOptions(&opts2), makePureOptions(&opts1))
	}
}

func TestHandshake1(t *testing.T) {
	testSetup(
		t,
		func(ctx context.Context, conn net.Conn) {
			st := new(Stream).Init(&Options{Transport: &transport.Options{Logger: &logger}}, false, uuid.UUID{}, nil, nil, nil)
			defer st.Close()
			ok, err := st.Establish(ctx, conn, testHandshaker{
				CbEmitHandshake: func() (Message, error) {
					msg := RawMessage("welcome")
					return &msg, nil
				},
				CbNewHandshake: func() Message {
					return new(RawMessage)
				},
				CbHandleHandshake: func(ctx context.Context, h Message) (bool, error) {
					msg := h.(*RawMessage)
					if string(*msg) != string("welcome too") {
						return false, nil
					}
					return true, nil
				},
			}.Init())
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.True(t, ok)
		},
		func(ctx context.Context, conn net.Conn) {
			st := new(Stream).Init(&Options{Transport: &transport.Options{Logger: &logger}}, true, uuid.UUID{}, nil, nil, nil)
			defer st.Close()
			ok, err := st.Establish(ctx, conn, testHandshaker{
				CbNewHandshake: func() Message {
					return new(RawMessage)
				},
				CbHandleHandshake: func(ctx context.Context, h Message) (bool, error) {
					msg := h.(*RawMessage)
					if string(*msg) != string("welcome") {
						return false, nil
					}
					return true, nil
				},
				CbEmitHandshake: func() (Message, error) {
					msg := RawMessage("welcome too")
					return &msg, nil
				},
			}.Init())
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
		func(ctx context.Context, conn net.Conn) {
			st := new(Stream).Init(&Options{Transport: &transport.Options{HandshakeTimeout: -1}}, false, uuid.UUID{}, nil, nil, nil)
			defer st.Close()
			ok, err := st.Establish(ctx, conn, testHandshaker{}.Init())
			if !assert.Regexp(t, "i/o timeout", err) {
				t.FailNow()
			}
			assert.False(t, ok)
		},
		func(ctx context.Context, conn net.Conn) {
			st := new(Stream).Init(&Options{Transport: &transport.Options{HandshakeTimeout: -1}}, true, uuid.UUID{}, nil, nil, nil)
			defer st.Close()
			ok, err := st.Establish(ctx, conn, testHandshaker{
				CbHandleHandshake: func(ctx context.Context, h Message) (bool, error) {
					<-ctx.Done()
					<-time.After(10 * time.Millisecond)
					return true, ctx.Err()
				},
			}.Init())
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
		func(ctx context.Context, conn net.Conn) {
			st := new(Stream).Init(&Options{Transport: &transport.Options{HandshakeTimeout: -1}}, false, uuid.UUID{}, nil, nil, nil)
			defer st.Close()
			ok, err := st.Establish(ctx, conn, testHandshaker{
				CbHandleHandshake: func(ctx context.Context, h Message) (bool, error) {
					<-ctx.Done()
					<-time.After(10 * time.Millisecond)
					return true, ctx.Err()
				},
			}.Init())
			if !assert.EqualError(t, err, "context deadline exceeded") {
				t.FailNow()
			}
			assert.False(t, ok)
		},
		func(ctx context.Context, conn net.Conn) {
			st := new(Stream).Init(&Options{Transport: &transport.Options{HandshakeTimeout: -1}}, true, uuid.UUID{}, nil, nil, nil)
			defer st.Close()
			ok, err := st.Establish(ctx, conn, testHandshaker{}.Init())
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			assert.True(t, ok)
		},
	)
}

func TestHandshake4(t *testing.T) {
	mp1 := testMessageProcessor{}.Init()
	mp2 := testMessageProcessor{}.Init()
	testSetup2(
		t,
		&Options{},
		&Options{},
		&mp1,
		&mp2,
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, defaultKeepaliveInterval, st.incomingKeepaliveInterval)
			assert.Equal(t, defaultKeepaliveInterval, st.outgoingKeepaliveInterval)
			assert.Equal(t, defaultConcurrencyLimit, st.incomingConcurrencyLimit)
			assert.Equal(t, defaultConcurrencyLimit, st.outgoingConcurrencyLimit)
			st.Abort(nil)
		},
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, defaultKeepaliveInterval, st.incomingKeepaliveInterval)
			assert.Equal(t, defaultKeepaliveInterval, st.outgoingKeepaliveInterval)
			assert.Equal(t, defaultConcurrencyLimit, st.incomingConcurrencyLimit)
			assert.Equal(t, defaultConcurrencyLimit, st.outgoingConcurrencyLimit)
			st.Abort(nil)
		},
	)
	testSetup2(
		t,
		&Options{Transport: &transport.Options{Logger: &logger}, OutgoingKeepaliveInterval: minKeepaliveInterval},
		&Options{Transport: &transport.Options{Logger: &logger}, IncomingKeepaliveInterval: minKeepaliveInterval + 2*time.Second},
		&mp1,
		&mp2,
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minKeepaliveInterval+2*time.Second, st.outgoingKeepaliveInterval)
			st.Abort(nil)
		},
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minKeepaliveInterval+2*time.Second, st.incomingKeepaliveInterval)
			st.Abort(nil)
		},
	)
	testSetup2(
		t,
		&Options{IncomingKeepaliveInterval: minKeepaliveInterval},
		&Options{OutgoingKeepaliveInterval: minKeepaliveInterval + 2*time.Second},
		&mp1,
		&mp2,
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minKeepaliveInterval+2*time.Second, st.incomingKeepaliveInterval)
			st.Abort(nil)
		},
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minKeepaliveInterval+2*time.Second, st.outgoingKeepaliveInterval)
			st.Abort(nil)
		},
	)
	testSetup2(
		t,
		&Options{OutgoingKeepaliveInterval: minKeepaliveInterval + 2*time.Second},
		&Options{IncomingKeepaliveInterval: minKeepaliveInterval},
		&mp1,
		&mp2,
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minKeepaliveInterval+2*time.Second, st.outgoingKeepaliveInterval)
			st.Abort(nil)
		},
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minKeepaliveInterval+2*time.Second, st.incomingKeepaliveInterval)
			st.Abort(nil)
		},
	)
	testSetup2(
		t,
		&Options{IncomingKeepaliveInterval: minKeepaliveInterval + 2*time.Second},
		&Options{OutgoingKeepaliveInterval: minKeepaliveInterval},
		&mp1,
		&mp2,
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minKeepaliveInterval+2*time.Second, st.incomingKeepaliveInterval)
			st.Abort(nil)
		},
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minKeepaliveInterval+2*time.Second, st.outgoingKeepaliveInterval)
			st.Abort(nil)
		},
	)
	testSetup2(
		t,
		&Options{OutgoingConcurrencyLimit: minConcurrencyLimit + 100},
		&Options{IncomingConcurrencyLimit: minConcurrencyLimit},
		&mp1,
		&mp2,
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minConcurrencyLimit, st.outgoingConcurrencyLimit)
			st.Abort(nil)
		},
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minConcurrencyLimit, st.incomingConcurrencyLimit)
			st.Abort(nil)
		},
	)
	testSetup2(
		t,
		&Options{IncomingConcurrencyLimit: minConcurrencyLimit + 100},
		&Options{OutgoingConcurrencyLimit: minConcurrencyLimit},
		&mp1,
		&mp2,
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minConcurrencyLimit, st.incomingConcurrencyLimit)
			st.Abort(nil)
		},
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minConcurrencyLimit, st.outgoingConcurrencyLimit)
			st.Abort(nil)
		},
	)
	testSetup2(
		t,
		&Options{OutgoingConcurrencyLimit: minConcurrencyLimit},
		&Options{IncomingConcurrencyLimit: minConcurrencyLimit + 100},
		&mp1,
		&mp2,
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minConcurrencyLimit, st.outgoingConcurrencyLimit)
			st.Abort(nil)
		},
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minConcurrencyLimit, st.incomingConcurrencyLimit)
			st.Abort(nil)
		},
	)
	testSetup2(
		t,
		&Options{IncomingConcurrencyLimit: minConcurrencyLimit},
		&Options{OutgoingConcurrencyLimit: minConcurrencyLimit + 100},
		&mp1,
		&mp2,
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minConcurrencyLimit, st.incomingConcurrencyLimit)
			st.Abort(nil)
		},
		func(ctx context.Context, st *Stream) {
			assert.Equal(t, minConcurrencyLimit, st.outgoingConcurrencyLimit)
			st.Abort(nil)
		},
	)
}

func TestPingAndPong1(t *testing.T) {
	const N = 1
	opts1 := Options{}
	opts2 := Options{}
	sn1 := int32(0)
	var mp1 testMessageProcessor
	mp1 = testMessageProcessor{
		CbPostEmitRequest: func(ev *Event) {
			sn1++
			if sn1 == N {
				mp1.Stream.Abort(nil)
			}
		},
		CbNewRequest: func(ev *Event) {
			ev.Message = new(RawMessage)
		},
		CbHandleRequest: func(ctx context.Context, ev *Event) {
			assert.Equal(t, "ping", string(*ev.Message.(*RawMessage)))
			resp := RawMessage("pong")
			ev.Err = mp1.Stream.SendResponse(&proto.ResponseHeader{
				SequenceNumber: ev.RequestHeader.SequenceNumber,
				RpcError: proto.RPCError{
					Type: proto.RPC_ERROR_NOT_IMPLEMENTED,
				},
			}, &resp)
		},
		CbNewResponse: func(ev *Event) {
			ev.Message = new(RawMessage)
		},
		CbHandleResponse: func(ctx context.Context, ev *Event) {
			assert.Equal(t, sn1-1, ev.ResponseHeader.SequenceNumber)
			assert.Equal(t, "pong", string(*ev.Message.(*RawMessage)))
			req := RawMessage("ping")
			ev.Err = mp1.Stream.SendRequest(ctx, &proto.RequestHeader{
				SequenceNumber: sn1,
			}, &req)
		},
	}.Init()
	cb1 := func(ctx context.Context, st *Stream) {
		req := RawMessage("ping")
		err := st.SendRequest(ctx, &proto.RequestHeader{}, &req)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}
	sn2 := int32(0)
	var mp2 testMessageProcessor
	mp2 = testMessageProcessor{
		CbPostEmitRequest: func(ev *Event) {
			sn2++
		},
		CbNewRequest: func(ev *Event) {
			ev.Message = new(RawMessage)
		},
		CbHandleRequest: func(ctx context.Context, ev *Event) {
			assert.Equal(t, "ping", string(*ev.Message.(*RawMessage)))
			resp := RawMessage("pong")
			ev.Err = mp2.Stream.SendResponse(&proto.ResponseHeader{
				SequenceNumber: ev.RequestHeader.SequenceNumber,
				RpcError: proto.RPCError{
					Type: proto.RPC_ERROR_NOT_IMPLEMENTED,
				},
			}, &resp)
		},
		CbNewResponse: func(ev *Event) {
			ev.Message = new(RawMessage)
		},
		CbHandleResponse: func(ctx context.Context, ev *Event) {
			assert.Equal(t, sn2-1, ev.ResponseHeader.SequenceNumber)
			assert.Equal(t, "pong", string(*ev.Message.(*RawMessage)))
			req := RawMessage("ping")
			ev.Err = mp2.Stream.SendRequest(ctx, &proto.RequestHeader{
				SequenceNumber: sn2,
			}, &req)
		},
	}.Init()
	cb2 := func(ctx context.Context, st *Stream) {
		req := RawMessage("ping")
		err := st.SendRequest(ctx, &proto.RequestHeader{}, &req)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
	}
	testSetup2(t, &opts1, &opts2, &mp1, &mp2, cb1, cb2)
}

func TestPingAndPong2(t *testing.T) {
	const N = 1000
	opts1 := Options{IncomingConcurrencyLimit: 10, Transport: &transport.Options{Logger: &logger}}
	opts2 := Options{Transport: &transport.Options{Logger: &logger}}
	sns1 := map[int32]struct{}{}
	sns2 := map[int32]struct{}{}
	cnt1 := int32(N)
	cnt2 := int32(N)
	var mp1 testMessageProcessor
	mp1 = testMessageProcessor{
		CbHandleRequest: func(ctx context.Context, ev *Event) {
			select {
			case <-ctx.Done():
				ev.Err = ctx.Err()
				return
			case <-time.After(time.Duration(ev.RequestHeader.SequenceNumber%3) * time.Millisecond):
			}
			resp := RawMessage(fmt.Sprintf("pong%d", ev.RequestHeader.SequenceNumber))
			ev.Err = mp1.Stream.SendResponse(&proto.ResponseHeader{
				SequenceNumber: ev.RequestHeader.SequenceNumber,
			}, &resp)
		},
		CbNewResponse: func(ev *Event) {
			ev.Message = new(RawMessage)
		},
		CbHandleResponse: func(ctx context.Context, ev *Event) {
			if assert.Equal(t, fmt.Sprintf("pong%d", ev.ResponseHeader.SequenceNumber), string(*ev.Message.(*RawMessage))) {
				_, ok := sns1[ev.ResponseHeader.SequenceNumber]
				if assert.False(t, ok) {
					sns1[ev.ResponseHeader.SequenceNumber] = struct{}{}
					if atomic.AddInt32(&cnt1, -1) == 0 && atomic.LoadInt32(&cnt2) == 0 {
						mp1.Stream.Abort(nil)
					}
				}
			}
		},
	}.Init()
	cb1 := func(ctx context.Context, st *Stream) {
		wg := sync.WaitGroup{}
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				req := RawMessage(fmt.Sprintf("ping%d", i))
				err := st.SendRequest(ctx, &proto.RequestHeader{SequenceNumber: int32(i)}, &req)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			}(i)
		}
		wg.Wait()
	}
	var mp2 testMessageProcessor
	mp2 = testMessageProcessor{
		CbHandleRequest: func(ctx context.Context, ev *Event) {
			select {
			case <-ctx.Done():
				ev.Err = ctx.Err()
				return
			case <-time.After(time.Duration(ev.RequestHeader.SequenceNumber%3) * time.Millisecond):
			}
			resp := RawMessage(fmt.Sprintf("pong%d", ev.RequestHeader.SequenceNumber))
			ev.Err = mp2.Stream.SendResponse(&proto.ResponseHeader{
				SequenceNumber: ev.RequestHeader.SequenceNumber,
			}, &resp)
		},
		CbNewResponse: func(ev *Event) {
			ev.Message = new(RawMessage)
		},
		CbHandleResponse: func(ctx context.Context, ev *Event) {
			if assert.Equal(t, fmt.Sprintf("pong%d", ev.ResponseHeader.SequenceNumber), string(*ev.Message.(*RawMessage))) {
				_, ok := sns2[ev.ResponseHeader.SequenceNumber]
				if assert.False(t, ok) {
					sns2[ev.ResponseHeader.SequenceNumber] = struct{}{}
					if atomic.AddInt32(&cnt2, -1) == 0 && atomic.LoadInt32(&cnt1) == 0 {
						mp1.Stream.Abort(nil)
					}
				}
			}
		},
	}.Init()
	cb2 := func(ctx context.Context, st *Stream) {
		wg := sync.WaitGroup{}
		for i := 0; i < N; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				req := RawMessage(fmt.Sprintf("ping%d", i))
				err := st.SendRequest(ctx, &proto.RequestHeader{SequenceNumber: int32(i)}, &req)
				if !assert.NoError(t, err) {
					t.FailNow()
				}
			}(i)
		}
		wg.Wait()
	}
	testSetup2(t, &opts1, &opts2, &mp1, &mp2, cb1, cb2)
}

func TestKeepalive(t *testing.T) {
	opts1 := Options{IncomingKeepaliveInterval: -1, OutgoingKeepaliveInterval: -1}
	opts2 := Options{IncomingKeepaliveInterval: -1, OutgoingKeepaliveInterval: -1}
	i := 0
	j := 0
	var mp1 testMessageProcessor
	mp1 = testMessageProcessor{
		CbNewKeepalive: func(ev *Event) {
			ev.Message = NullMessage
		},
		CbHandleKeepalive: func(ctx context.Context, ev *Event) {
			i++
			if i == 2 {
				mp1.Stream.Abort(nil)
			}
		},
		CbEmitKeepalive: func(ev *Event) {
			j++
			ev.Message = NullMessage
			if j == 2 {
				mp1.Stream.Abort(nil)
			}
		},
	}.Init()
	cb1 := func(ctx context.Context, st *Stream) {
	}
	var mp2 testMessageProcessor
	mp2 = testMessageProcessor{
		CbNewKeepalive: func(ev *Event) {
			ev.Message = NullMessage
		},
		CbHandleKeepalive: func(ctx context.Context, ev *Event) {
			ev.Message = NullMessage
		},
		CbEmitKeepalive: func(ev *Event) {
			ev.Message = NullMessage
		},
	}.Init()
	cb2 := func(ctx context.Context, st *Stream) {
	}
	testSetup2(t, &opts1, &opts2, &mp1, &mp2, cb1, cb2)
	assert.Greater(t, i, 0)
	assert.Greater(t, j, 0)
}

func testSetup(
	t *testing.T,
	cb1 func(ctx context.Context, conn net.Conn),
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
		conn, err := net.Dial("tcp", l.Addr().String())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		cb1(ctx, conn)
	}()
	defer wg.Wait()
	conn, err := l.Accept()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	cb2(ctx, conn)
}

func testSetup2(
	t *testing.T,
	opts1 *Options,
	opts2 *Options,
	mp1 *testMessageProcessor,
	mp2 *testMessageProcessor,
	cb1 func(ctx context.Context, st *Stream),
	cb2 func(ctx context.Context, st *Stream),
) {
	testSetup(
		t,
		func(ctx context.Context, conn net.Conn) {
			st := new(Stream).Init(opts1, false, uuid.UUID{}, nil, nil, nil)
			defer st.Close()
			ok, err := st.Establish(ctx, conn, testHandshaker{}.Init())
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			if !assert.True(t, ok) {
				t.FailNow()
			}
			mp1.Stream = st
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := st.Process(ctx, &testTrafficCrypter{"admin", 0}, mp1)
				t.Log(err)
			}()
			defer wg.Wait()
			cb1(ctx, st)
		},
		func(ctx context.Context, conn net.Conn) {
			st := new(Stream).Init(opts2, true, uuid.UUID{}, nil, nil, nil)
			defer st.Close()
			ok, err := st.Establish(ctx, conn, testHandshaker{}.Init())
			if !assert.NoError(t, err) {
				t.FailNow()
			}
			if !assert.True(t, ok) {
				t.FailNow()
			}
			mp2.Stream = st
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := st.Process(ctx, &testTrafficCrypter{"admin", 0}, mp2)
				t.Log(err)
			}()
			defer wg.Wait()
			cb2(ctx, st)
		},
	)
}

type testHandshaker struct {
	CbNewHandshake    func() Message
	CbHandleHandshake func(context.Context, Message) (bool, error)
	CbEmitHandshake   func() (Message, error)
}

func (s testHandshaker) Init() testHandshaker {
	if s.CbNewHandshake == nil {
		s.CbNewHandshake = func() Message { return NullMessage }
	}
	if s.CbHandleHandshake == nil {
		s.CbHandleHandshake = func(context.Context, Message) (bool, error) { return true, nil }
	}
	if s.CbEmitHandshake == nil {
		s.CbEmitHandshake = func() (Message, error) { return NullMessage, nil }
	}
	return s
}

func (s testHandshaker) NewHandshake() Message {
	return s.CbNewHandshake()
}

func (s testHandshaker) HandleHandshake(ctx context.Context, h Message) (ok bool, err error) {
	return s.CbHandleHandshake(ctx, h)
}

func (s testHandshaker) EmitHandshake() (Message, error) {
	return s.CbEmitHandshake()
}

type testTrafficCrypter struct {
	key  string
	keyi int
}

func (s *testTrafficCrypter) EncryptTraffic(traffic []byte) {
	for i := range traffic {
		traffic[i] ^= s.key[s.keyi%len(s.key)]
	}
}

func (s *testTrafficCrypter) DecryptTraffic(traffic []byte) {
	for i := range traffic {
		traffic[i] ^= s.key[s.keyi%len(s.key)]
	}
}

type testMessageProcessor struct {
	CbNewKeepalive     func(*Event)
	CbHandleKeepalive  func(context.Context, *Event)
	CbEmitKeepalive    func(*Event)
	CbNewRequest       func(*Event)
	CbHandleRequest    func(context.Context, *Event)
	CbPostEmitRequest  func(*Event)
	CbNewResponse      func(*Event)
	CbHandleResponse   func(context.Context, *Event)
	CbPostEmitResponse func(*Event)
	Stream             *Stream
}

func (s testMessageProcessor) Init() testMessageProcessor {
	if s.CbNewKeepalive == nil {
		s.CbNewKeepalive = func(ev *Event) { ev.Message = NullMessage }
	}
	if s.CbHandleKeepalive == nil {
		s.CbHandleKeepalive = func(context.Context, *Event) {}
	}
	if s.CbEmitKeepalive == nil {
		s.CbEmitKeepalive = func(ev *Event) { ev.Message = NullMessage }
	}
	if s.CbNewRequest == nil {
		s.CbNewRequest = func(ev *Event) { ev.Message = NullMessage }
	}
	if s.CbHandleRequest == nil {
		s.CbHandleRequest = func(context.Context, *Event) {}
	}
	if s.CbPostEmitRequest == nil {
		s.CbPostEmitRequest = func(*Event) {}
	}
	if s.CbNewResponse == nil {
		s.CbNewResponse = func(ev *Event) { ev.Message = NullMessage }
	}
	if s.CbHandleResponse == nil {
		s.CbHandleResponse = func(context.Context, *Event) {}
	}
	if s.CbPostEmitResponse == nil {
		s.CbPostEmitResponse = func(*Event) {}
	}
	return s
}

func (s testMessageProcessor) NewKeepalive(ev *Event) {
	s.CbNewKeepalive(ev)
}

func (s testMessageProcessor) HandleKeepalive(ctx context.Context, ev *Event) {
	s.CbHandleKeepalive(ctx, ev)
}

func (s testMessageProcessor) EmitKeepalive(ev *Event) {
	s.CbEmitKeepalive(ev)
}

func (s testMessageProcessor) NewRequest(ev *Event) {
	s.CbNewRequest(ev)
}

func (s testMessageProcessor) HandleRequest(ctx context.Context, ev *Event) {
	s.CbHandleRequest(ctx, ev)
}

func (s testMessageProcessor) PostEmitRequest(ev *Event) {
	s.CbPostEmitRequest(ev)
}

func (s testMessageProcessor) NewResponse(ev *Event) {
	s.CbNewResponse(ev)
}

func (s testMessageProcessor) HandleResponse(ctx context.Context, ev *Event) {
	s.CbHandleResponse(ctx, ev)
}

func (s testMessageProcessor) PostEmitResponse(ev *Event) {
	s.CbPostEmitResponse(ev)
}

var logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()
