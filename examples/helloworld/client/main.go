package main

import (
	"context"
	"fmt"

	"github.com/let-z-go/gogorpc/client"
	"github.com/let-z-go/gogorpc/examples/helloworld/proto"
)

func main() {
	opts := client.Options{
		ConnectRetry: client.ConnectRetryOptions{
			MaxCount: 2,
		},
	}

	cli := new(client.Client).Init(&opts, "tcp://127.0.0.1:8888")
	defer cli.Close()
	stub := new(proto.GreeterStub).Init(cli)

	for _, name := range []string{"tom", "jerry", "spike"} {
		resp, err := stub.SayHello(context.Background(), &proto.SayHelloReq{
			Name: name,
		})

		if err == nil {
			fmt.Println("resp:", resp)
		} else {
			if proto.RPCErrForbiddenName.Equals(err) {
				fmt.Println("forbidden name:", name)
			} else {
				fmt.Println("err:", err)
			}
		}
	}
}
