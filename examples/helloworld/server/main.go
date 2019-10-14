package main

import (
	"context"

	"github.com/let-z-go/gogorpc/channel"
	"github.com/let-z-go/gogorpc/examples/helloworld/proto"
	"github.com/let-z-go/gogorpc/server"
)

type Greeter struct{}

func (Greeter) SayHello(ctx context.Context, request *proto.SayHelloReq) (*proto.SayHelloResp, error) {
	if request.Name == "spike" {
		return nil, proto.RPCErrForbiddenName
	}

	return &proto.SayHelloResp{
		Message: "Hello " + request.Name,
	}, nil
}

func (Greeter) SayHello2(ctx context.Context, request *proto.SayHelloReq) error {
	return channel.RPCErrNotImplemented
}

func (Greeter) SayHello3(ctx context.Context) (*proto.SayHelloResp, error) {
	return nil, channel.RPCErrNotImplemented
}

func main() {
	opts := server.Options{
		Channel: (&channel.Options{}).
			Do(proto.ImplementGreeter(Greeter{})),
	}

	svr := new(server.Server).Init(&opts, "tcp://127.0.0.1:8888")
	defer svr.Close()
	svr.Run()
}
