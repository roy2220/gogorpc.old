package main

import (
	"context"
	"fmt"

	"github.com/let-z-go/pbrpc"
	"github.com/let-z-go/pbrpc/sample"
)

type ServerServiceHandler struct {
	sample.ServerServiceHandlerBase
}

func (ServerServiceHandler) SayHello(context_ context.Context, channel pbrpc.Channel, request *sample.SayHelloRequest) (*sample.SayHelloResponse, error) {
	client := sample.ClientServiceClient{channel, context_}
	response, _ := client.GetNickname(true)

	response2 := &sample.SayHelloResponse{
		Reply: fmt.Sprintf(request.ReplyFormat, response.Nickname),
	}

	return response2, nil
}

func main() {
	serverPolicy := (&pbrpc.ServerPolicy{}).RegisterServiceHandler(ServerServiceHandler{})
	server := (&pbrpc.Server{}).Initialize(serverPolicy, "127.0.0.1:8888", "", nil)
	server.Run()
}
