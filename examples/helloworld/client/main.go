package main

import (
	"context"
	"fmt"

	"github.com/let-z-go/gogorpc/client"
	"github.com/let-z-go/gogorpc/examples/helloworld/protocol"
)

func main() {
	opts := client.Options{
		ConnectRetry: client.ConnectRetryOptions{
			MaxCount: 2,
		},
	}

	cli := new(client.Client).Init(&opts, "tcp://127.0.0.1:8888")
	defer cli.Close()

	for _, name := range []string{"tom", "jerry", "spike"} {
		req := protocol.SayHelloReq{Name: name}
		stub := new(protocol.GreeterStub).Init(cli)
		resp, err := stub.SayHello(context.Background(), &req)

		if err == nil {
			fmt.Println("resp:", resp)
		} else {
			if protocol.RPCErrForbiddenName.Equals(err) {
				fmt.Println("forbidden name:", name)
			} else {
				fmt.Println("err:", err)
			}
		}
	}
}
