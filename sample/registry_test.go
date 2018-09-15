package sample

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/let-z-go/pbrpc"
	"github.com/let-z-go/toolkit/logger"
	"github.com/let-z-go/zk"
)

type ServerServiceHandler1 struct {
	ServerServiceHandlerBase
}

func (ServerServiceHandler1) SayHello(context_ context.Context, request *SayHelloRequest) (*SayHelloResponse, error) {
	response := &SayHelloResponse{
		Reply: fmt.Sprintf(request.ReplyFormat, "0"),
	}

	return response, nil
}

type ServerServiceHandler2 struct {
	ServerServiceHandlerBase
}

func (ServerServiceHandler2) SayHello(context_ context.Context, request *SayHelloRequest) (*SayHelloResponse, error) {
	response := &SayHelloResponse{
		Reply: fmt.Sprintf(request.ReplyFormat, "1"),
	}

	return response, nil
}

type ServerServiceHandler3 struct {
	ServerServiceHandlerBase
}

func (ServerServiceHandler3) SayHello(context_ context.Context, request *SayHelloRequest) (*SayHelloResponse, error) {
	response := &SayHelloResponse{
		Reply: fmt.Sprintf(request.ReplyFormat, "2"),
	}

	return response, nil
}

func TestRegistry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	zksp := &zk.SessionPolicy{Logger: *(&logger.Logger{}).Initialize("zk", logger.SeverityInfo, os.Stdout, os.Stderr)}
	zkc := (&zk.Client{}).Initialize(zksp, []string{"192.168.33.1:2181"}, nil, nil, "/", ctx)
	cp := &pbrpc.ChannelPolicy{Logger: *(&logger.Logger{}).Initialize("pbrpc-cli", logger.SeverityInfo, os.Stdout, os.Stderr)}
	reg := (&pbrpc.Registry{}).Initialize(zkc, cp, ctx)
	sp1 := (&pbrpc.ServerPolicy{Registry: reg, Channel: pbrpc.ChannelPolicy{Logger: *(&logger.Logger{}).Initialize("pbrpc-srv1", logger.SeverityInfo, os.Stdout, os.Stderr)}}).
		RegisterServiceHandler(&ServerServiceHandler1{})
	s1 := (&pbrpc.Server{}).Initialize(sp1, "127.0.0.1:0", "", ctx)
	sp2 := (&pbrpc.ServerPolicy{Registry: reg, Channel: pbrpc.ChannelPolicy{Logger: *(&logger.Logger{}).Initialize("pbrpc-srv2", logger.SeverityInfo, os.Stdout, os.Stderr)}}).
		RegisterServiceHandler(&ServerServiceHandler2{})
	s2 := (&pbrpc.Server{}).Initialize(sp2, "127.0.0.1:0", "", ctx)
	sp3 := (&pbrpc.ServerPolicy{Registry: reg, Channel: pbrpc.ChannelPolicy{Logger: *(&logger.Logger{}).Initialize("pbrpc-srv3", logger.SeverityInfo, os.Stdout, os.Stderr)}}).
		RegisterServiceHandler(&ServerServiceHandler3{})
	s3 := (&pbrpc.Server{}).Initialize(sp3, "127.0.0.1:0", "", ctx)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		zkc.Run()
		wg.Done()
	}()

	wg.Add(1)

	go func() {
		s1.Run()
		wg.Done()
	}()

	wg.Add(1)

	go func() {
		s2.Run()
		wg.Done()
	}()

	wg.Add(1)

	go func() {
		s3.Run()
		wg.Done()
	}()

	var wg2 sync.WaitGroup
	wg2.Add(1)

	go func() {
		time.Sleep(3 * time.Second)
		mc := reg.GetMethodCaller(pbrpc.LBRoundRobin, 0)
		r := SayHelloRequest{"%v"}
		var a, b, c int32
		cs := []*int32{&a, &b, &c}
		var wg3 sync.WaitGroup

		for m := 100; m >= 1; m-- {
			wg3.Add(1)

			go func(m int) {
				for n := 60; n >= 1; n-- {
					rsp, e := ServerServiceClient{mc, nil}.SayHello(&r, true)

					if e == nil {
						i := rsp.Reply[0] - '0'
						atomic.AddInt32(cs[i], 1)
					} else {
						t.Errorf("%v", e)
					}

					// if m*n == 3330 {
					//	s2.Stop(true)
					// }
				}

				wg3.Done()
			}(m)
		}

		wg3.Wait()

		if a != b || b != c {
			t.Errorf("a=%#v, b=%#v, c=%#v", a, b, c)
		}

		wg2.Done()
	}()

	wg2.Add(1)

	go func() {
		time.Sleep(3 * time.Second)
		mc := reg.GetMethodCaller(pbrpc.LBRandomized, 0)
		r := SayHelloRequest{"%v"}
		var a, b, c int32
		cs := []*int32{&a, &b, &c}
		var wg3 sync.WaitGroup

		for m := 100; m >= 1; m-- {
			wg3.Add(1)

			go func(m int) {
				for n := 60; n >= 1; n-- {
					rsp, e := ServerServiceClient{mc, nil}.SayHello(&r, true)

					if e == nil {
						i := rsp.Reply[0] - '0'
						atomic.AddInt32(cs[i], 1)
					} else {
						t.Errorf("%v", e)
					}

					// if m*n == 4440 {
					//	s3.Stop(true)
					// }
				}

				wg3.Done()
			}(m)
		}

		wg3.Wait()

		if (a-b)*(a-b) >= 160000 || (b-c)*(b-c) >= 160000 || (c-a)*(c-a) >= 160000 {
			t.Errorf("a=%#v, b=%#v, c=%#v", a, b, c)
		}

		wg2.Done()
	}()

	wg2.Add(1)

	go func() {
		time.Sleep(3 * time.Second)
		mc := reg.GetMethodCaller(pbrpc.LBConsistentHashing, uintptr(time.Now().Unix()))
		r := SayHelloRequest{"%v"}
		var a, b, c int32
		cs := []*int32{&a, &b, &c}
		var wg3 sync.WaitGroup

		for m := 100; m >= 1; m-- {
			wg3.Add(1)

			go func(m int) {
				for n := 60; n >= 1; n-- {
					rsp, e := ServerServiceClient{mc, nil}.SayHello(&r, true)

					if e == nil {
						i := rsp.Reply[0] - '0'
						atomic.AddInt32(cs[i], 1)
					} else {
						t.Errorf("%v", e)
					}

					// if m*n == 2220 {
					//	s1.Stop(true)
					// }
				}

				wg3.Done()
			}(m)
		}

		wg3.Wait()

		if b < a {
			a, b = b, a
		}

		if c < a {
			a, c = c, a
		}

		if b > c {
			b, c = c, b
		}

		if a != 0 || b != 0 || c != 6000 {
			t.Errorf("a=%#v, b=%#v, c=%#v", a, b, c)
		}

		wg2.Done()
	}()

	wg2.Wait()
	cancel()
	wg.Wait()
}

func BenchmarkRegistry(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	zksp := &zk.SessionPolicy{Logger: *(&logger.Logger{}).Initialize("zk", logger.SeverityInfo, os.Stdout, os.Stderr)}
	zkc := (&zk.Client{}).Initialize(zksp, []string{"192.168.33.1:2181"}, nil, nil, "/", ctx)
	cp := &pbrpc.ChannelPolicy{Logger: *(&logger.Logger{}).Initialize("pbrpc-cli", logger.SeverityInfo, os.Stdout, os.Stderr)}
	reg := (&pbrpc.Registry{}).Initialize(zkc, cp, ctx)
	sp1 := (&pbrpc.ServerPolicy{Registry: reg, Channel: pbrpc.ChannelPolicy{Logger: *(&logger.Logger{}).Initialize("pbrpc-srv1", logger.SeverityInfo, os.Stdout, os.Stderr)}}).
		RegisterServiceHandler(&ServerServiceHandler1{})
	s1 := (&pbrpc.Server{}).Initialize(sp1, "127.0.0.1:0", "", ctx)
	sp2 := (&pbrpc.ServerPolicy{Registry: reg, Channel: pbrpc.ChannelPolicy{Logger: *(&logger.Logger{}).Initialize("pbrpc-srv2", logger.SeverityInfo, os.Stdout, os.Stderr)}}).
		RegisterServiceHandler(&ServerServiceHandler2{})
	s2 := (&pbrpc.Server{}).Initialize(sp2, "127.0.0.1:0", "", ctx)
	sp3 := (&pbrpc.ServerPolicy{Registry: reg, Channel: pbrpc.ChannelPolicy{Logger: *(&logger.Logger{}).Initialize("pbrpc-srv3", logger.SeverityInfo, os.Stdout, os.Stderr)}}).
		RegisterServiceHandler(&ServerServiceHandler3{})
	s3 := (&pbrpc.Server{}).Initialize(sp3, "127.0.0.1:0", "", ctx)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		zkc.Run()
		wg.Done()
	}()

	wg.Add(1)

	go func() {
		s1.Run()
		wg.Done()
	}()

	wg.Add(1)

	go func() {
		s2.Run()
		wg.Done()
	}()

	wg.Add(1)

	go func() {
		s3.Run()
		wg.Done()
	}()

	wg.Add(1)

	go func() {
		time.Sleep(3 * time.Second)
		mc := reg.GetMethodCaller(pbrpc.LBRoundRobin, 0)
		r := SayHelloRequest{"%v"}
		var wg2 sync.WaitGroup

		for m := 5000; m >= 1; m-- {
			wg2.Add(1)

			go func(m int) {
				for n := 200; n >= 1; n-- {
					_, e := ServerServiceClient{mc, nil}.SayHello(&r, true)

					if e != nil {
						b.Errorf("%v", e)
					}
				}

				wg2.Done()
			}(m)
		}

		wg2.Wait()
		cancel()
		wg.Done()
	}()

	wg.Wait()
}
