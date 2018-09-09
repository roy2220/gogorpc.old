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

func (*ServerServiceHandler) SayHello(context_ context.Context, request *sample.SayHelloRequest) (*sample.SayHelloResponse, error) {
	contextVars := pbrpc.MustGetContextVars(context_)
	client := sample.ClientServiceClient{contextVars.Channel, context_}
	response, _ := client.GetNickname(true)

	response2 := &sample.SayHelloResponse{
		Reply: fmt.Sprintf(request.ReplyFormat, response.Nickname),
	}

	return response2, nil
}

func InterceptMethod(methodHandlingInfo *pbrpc.MethodHandlingInfo, methodhandler pbrpc.MethodHandler) (pbrpc.OutgoingMessage, pbrpc.ErrorCode) {
	serviceName := methodHandlingInfo.ServiceHandler.X_GetName()
	methodName := methodHandlingInfo.MethodRecord.Name
	fmt.Printf("%v.%v begin\n", serviceName, methodName)
	fmt.Printf("request=%#v\n", methodHandlingInfo.Request)
	response, e := methodhandler(methodHandlingInfo)
	fmt.Printf("response=%#v\n", response)
	fmt.Printf("%v.%v end\n", serviceName, methodName)
	return response, e
}

func main() {
	serviceHandler := pbrpc.RegisterMethodInterceptors(&ServerServiceHandler{}, InterceptMethod)
	serverPolicy := (&pbrpc.ServerPolicy{}).RegisterServiceHandler(serviceHandler)
	server := (&pbrpc.Server{}).Initialize(serverPolicy, "127.0.0.1:8888", "", nil)
	server.Run()
}
