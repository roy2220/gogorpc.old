package server

import (
	"context"
	"fmt"

	"github.com/let-z-go/gogorpc/channel"
	"github.com/let-z-go/gogorpc/client"
)

func ExampleServer() {
	// run server
	chOpts := channel.Options{}

	chOpts.BuildMethod("foo", "bar").
		SetRequestFactory(func() channel.Message {
			return new(channel.RawMessage)
		}).
		SetIncomingRPCHandler(func(rpc *channel.RPC) {
			req := rpc.Request.(*channel.RawMessage)
			resp := channel.RawMessage(string(*req) + " too")
			fmt.Println("-- response:", string(resp))
			rpc.Response = &resp
		}).
		AddIncomingRPCInterceptor(func(rpc *channel.RPC) {
			req := rpc.Request.(*channel.RawMessage)
			fmt.Println("-- incoming request intercepted:", string(*req))
			rpc.Handle()
			resp2 := channel.RawMessage(string(*req) + " too")
			rpc.Response = &resp2
			resp := rpc.Response.(*channel.RawMessage)
			fmt.Println("-- outgoing response intercepted:", string(*resp))
		})

	opts := Options{Channel: &chOpts}
	svr := new(Server).Init(&opts, "tcp://127.0.0.1:8888")
	defer svr.Close()
	go svr.Run()

	// run client
	cli := new(client.Client).Init(&client.Options{}, "tcp://127.0.0.1:8888")
	defer cli.Close()
	req := channel.RawMessage("hello world")

	rpc := channel.RPC{
		Ctx:         context.Background(),
		ServiceName: "foo",
		MethodName:  "bar",
		Request:     &req,
	}

	cli.DoRPC(&rpc, channel.NewRawMessage)
	resp := rpc.Response.(*channel.RawMessage)
	fmt.Println(string(*resp))
	// Output:
	// -- incoming request intercepted: hello world
	// -- response: hello world too
	// -- outgoing response intercepted: hello world too
	// hello world too
}
