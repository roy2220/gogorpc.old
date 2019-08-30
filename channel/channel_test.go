package channel

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"

	"github.com/let-z-go/pbrpc/internal/transport"
)

func TestPingAndPong(t *testing.T) {
	const N = 4000
	opts := Options{Stream: &StreamOptions{
		LocalConcurrencyLimit:  100,
		RemoteConcurrencyLimit: 100,
		Transport:              &transport.Options{Logger: &logger},
	}}
	opts.SetMethod("service1", "method1").
		SetRequestFactory(func() Message { return new(RawMessage) }).
		SetResponseFactory(func() Message { return new(RawMessage) }).
		SetIncomingRPCHandler(func(rpc *RPC) {
			msg := RawMessage(fmt.Sprintf("return service1.method1(%s)", *rpc.Request.(*RawMessage)))
			rpc.Response = &msg
		})
	opts.SetMethod("service2", "method2").
		SetRequestFactory(func() Message { return new(RawMessage) }).
		SetResponseFactory(func() Message { return new(RawMessage) }).
		SetIncomingRPCHandler(func(rpc *RPC) {
			msg := RawMessage(fmt.Sprintf("return service2.method2(%s)", *rpc.Request.(*RawMessage)))
			rpc.Response = &msg
		})
	testSetup2(
		t,
		&opts,
		&opts,
		func(ctx context.Context, cn *Channel) {
			wg := sync.WaitGroup{}
			for i := 0; i < N; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					msg := RawMessage(fmt.Sprintf("req2:%d", i))
					rpc := RPC{
						Ctx:         ctx,
						ServiceName: "service2",
						MethodName:  "method2",
						Request:     &msg,
					}
					cn.InvokeRPC(&rpc)
					if !assert.NoError(t, rpc.Err) {
						t.FailNow()
					}
					assert.Equal(t, fmt.Sprintf("return service2.method2(%s)", msg), string(*rpc.Response.(*RawMessage)))
				}(i)
			}
			wg.Wait()
			time.Sleep(1 * time.Second)
			cn.Abort(nil)
		},
		func(ctx context.Context, cn *Channel, conn net.Conn) bool {
			wg := sync.WaitGroup{}
			for i := 0; i < N; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					msg := RawMessage(fmt.Sprintf("req1:%d", i))
					rpc := RPC{
						Ctx:         ctx,
						ServiceName: "service1",
						MethodName:  "method1",
						Request:     &msg,
					}
					cn.InvokeRPC(&rpc)
					if !assert.NoError(t, rpc.Err) {
						t.FailNow()
					}
					assert.Equal(t, fmt.Sprintf("return service1.method1(%s)", msg), string(*rpc.Response.(*RawMessage)))
				}(i)
			}
			wg.Wait()
			time.Sleep(1 * time.Second)
			cn.Abort(nil)
			return false
		},
		0,
	)
}

func TestBadHandshake(t *testing.T) {
	testSetup2(
		t,
		&Options{Handshaker: testHandshaker{
			CbNewHandshake: func() Message {
				return new(RawMessage)
			},
			CbHandleHandshake: func(ctx context.Context, hp Message) (bool, error) {
				if string(*hp.(*RawMessage)) == "false" {
					return false, nil
				}
				return true, nil
			},
		}.Init()},
		&Options{Handshaker: testHandshaker{
			CbEmitHandshake: func() (Message, error) {
				msg := RawMessage("false")
				return &msg, nil
			},
		}.Init()},
		func(ctx context.Context, cn *Channel) {
			for i := 0; i < 10; i++ {
				rpc := RPC{
					Ctx:         ctx,
					ServiceName: "service2",
					MethodName:  "method2",
					Request:     NullMessage,
				}
				cn.InvokeRPC(&rpc)
				assert.EqualError(t, rpc.Err, "pbrpc/channel: closed")
			}
			cn.Abort(nil)
		},
		func(ctx context.Context, cn *Channel, conn net.Conn) bool {
			return false
		},
		0,
	)
}

func TestBroken(t *testing.T) {
	opts2 := &Options{}
	opts2.SetMethod("1", "2").SetIncomingRPCHandler(func(rpc *RPC) {
		if rpc.RequestExtraData["I"][0]%2 == 0 {
			<-rpc.Ctx.Done()
			rpc.Response = NullMessage
		} else {
			rpc.Response = NullMessage
		}
	})
	f := false
	testSetup2(
		t,
		&Options{},
		opts2,
		func(ctx context.Context, cn *Channel) {
			wg := sync.WaitGroup{}
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					rpc := RPC{
						Ctx:              ctx,
						ServiceName:      "1",
						MethodName:       "2",
						Request:          NullMessage,
						RequestExtraData: ExtraData{"I": []byte{byte(i)}},
					}
					cn.InvokeRPC(&rpc)
					if i%2 == 0 {
						assert.EqualError(t, rpc.Err, "pbrpc/channel: broken")
					}
				}(i)
			}
			wg.Wait()
			cn.Abort(nil)
		},
		func(ctx context.Context, cn *Channel, conn net.Conn) bool {
			if !f {
				f = true
				time.Sleep(time.Second)
				conn.Close()
				return true
			}
			return false
		},
		0,
	)
}

func TestReconnection1(t *testing.T) {
	f := false
	testSetup2(
		t,
		&Options{},
		&Options{},
		func(ctx context.Context, cn *Channel) {
			wg := sync.WaitGroup{}
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					rpc := RPC{
						Ctx:         ctx,
						ServiceName: "1",
						MethodName:  "2",
						Request:     NullMessage,
					}
					if i < 5 {
						if i == 0 {
							rpc.Request = testBlockMessage{time.Second / 2 * 3}
						}
						cn.InvokeRPC(&rpc)
						assert.EqualError(t, rpc.Err, "pbrpc/channel: broken")
					} else {
						time.Sleep(time.Second / 2)
						cn.InvokeRPC(&rpc)
						assert.EqualError(t, rpc.Err, "pbrpc/channel: rpc: not found")
					}
				}(i)
			}
			wg.Wait()
			cn.Abort(nil)
		},
		func(ctx context.Context, cn *Channel, conn net.Conn) bool {
			if !f {
				f = true
				time.Sleep(time.Second)
				conn.Close()
				return true
			}
			return false
		},
		0,
	)
}

func TestReconnection2(t *testing.T) {
	testSetup2(
		t,
		&Options{},
		&Options{},
		func(ctx context.Context, cn *Channel) {
			wg := sync.WaitGroup{}
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					rpc := RPC{
						Ctx:         ctx,
						ServiceName: "1",
						MethodName:  "2",
						Request:     NullMessage,
					}
					if i < 5 {
						if i == 0 {
							rpc.Request = testBlockMessage{time.Second / 2 * 3}
						}
						cn.InvokeRPC(&rpc)
						assert.EqualError(t, rpc.Err, "pbrpc/channel: broken")
					} else {
						time.Sleep(time.Second / 2)
						cn.InvokeRPC(&rpc)
						assert.EqualError(t, rpc.Err, "pbrpc/channel: closed")
					}
				}(i)
			}
			wg.Wait()
			cn.Abort(nil)
		},
		func(ctx context.Context, cn *Channel, conn net.Conn) bool {
			time.Sleep(time.Second)
			conn.Close()
			return false
		},
		0,
	)
}

func testSetup2(
	t *testing.T,
	opts1 *Options,
	opts2 *Options,
	cb1 func(ctx context.Context, cn *Channel),
	cb2 func(ctx context.Context, cn *Channel, conn net.Conn) bool,
	nMaxReconnect int,
) {
	testSetup(
		t,
		func(ctx context.Context, sa string) {
			cn := new(Channel).Init(opts1)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := cn.ConnectAndServe(ctx, MakeSimpleServerAddressProvider([]string{sa}, nMaxReconnect, 200*time.Millisecond))
				t.Log(err)
			}()
			defer wg.Wait()
			cb1(ctx, cn)
		},
		func(ctx context.Context, conn net.Conn) bool {
			cn := new(Channel).Init(opts2)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := cn.AcceptAndServe(ctx, conn)
				t.Log(err)
			}()
			defer wg.Wait()
			return cb2(ctx, cn, conn)
		},
	)
}

func testSetup(
	t *testing.T,
	cb1 func(ctx context.Context, sa string),
	cb2 func(ctx context.Context, conn net.Conn) bool,
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
	for {
		c, err := l.Accept()
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		if !cb2(ctx, c) {
			break
		}
	}
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
		s.CbHandleHandshake = func(ctx context.Context, hp Message) (bool, error) { return true, nil }
	}
	if s.CbEmitHandshake == nil {
		s.CbEmitHandshake = func() (Message, error) { return NullMessage, nil }
	}
	return s
}

func (s testHandshaker) NewHandshake() Message {
	return s.CbNewHandshake()
}

func (s testHandshaker) HandleHandshake(ctx context.Context, hp Message) (bool, error) {
	return s.CbHandleHandshake(ctx, hp)
}

func (s testHandshaker) EmitHandshake() (Message, error) {
	return s.CbEmitHandshake()
}

var logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout}).With().Timestamp().Logger()

type testBlockMessage struct{ dur time.Duration }

func (testBlockMessage) Unmarshal([]byte) error {
	return nil
}

func (testBlockMessage) Size() int {
	return 0
}

func (s testBlockMessage) MarshalTo([]byte) (int, error) {
	time.Sleep(s.dur)
	return 0, nil
}
