package main

import (
	"context"

	"github.com/let-z-go/gogorpc/channel"
	"github.com/let-z-go/gogorpc/examples/helloworld/protocol"
	"github.com/let-z-go/gogorpc/server"
)

type GreeterHandler struct{}

func (GreeterHandler) SayHello(ctx context.Context, request *protocol.SayHelloReq) (*protocol.SayHelloResp, error) {
	if request.Name == "spike" {
		return nil, protocol.RPCErrForbiddenName
	}

	return &protocol.SayHelloResp{
		Message: "Hello " + request.Name,
	}, nil
}

func (GreeterHandler) SayHello2(ctx context.Context, request *protocol.SayHelloReq) error {
	return channel.RPCErrNotImplemented
}

func (GreeterHandler) SayHello3(ctx context.Context) (*protocol.SayHelloResp, error) {
	return nil, channel.RPCErrNotImplemented
}

func main() {
	opts := server.Options{
		Channel: (&channel.Options{}).
			Do(protocol.RegisterGreeterHandler(GreeterHandler{})),
	}

	svr := new(server.Server).Init(&opts, "tcp://127.0.0.1:8888")
	defer svr.Close()
	svr.Run()
}
