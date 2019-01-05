package main

import (
	"context"
	"fmt"
	"os"

	"github.com/let-z-go/pbrpc"
	"github.com/let-z-go/pbrpc/sample"
	"github.com/let-z-go/toolkit/logger"
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

func InterceptMethod(methodHandlingInfo *pbrpc.MethodHandlingInfo, methodHandler pbrpc.MethodHandler) (pbrpc.OutgoingMessage, pbrpc.ErrorCode) {
	serviceName := methodHandlingInfo.ServiceHandler.X_GetName()
	methodName := methodHandlingInfo.MethodRecord.Name
	fmt.Printf("%v.%v begin\n", serviceName, methodName)
	fmt.Printf("trace_id=%#v\n", methodHandlingInfo.ContextVars.TraceID.Base64())
	fmt.Printf("request=%#v\n", methodHandlingInfo.Request)
	response, errorCode := methodHandler(methodHandlingInfo)
	fmt.Printf("response=%#v\n", response)
	fmt.Printf("%v.%v end\n", serviceName, methodName)
	return response, errorCode
}

func main() {
	serviceHandler := pbrpc.RegisterMethodInterceptors(&ServerServiceHandler{}, InterceptMethod)
	serverPolicy := (&pbrpc.ServerPolicy{Acceptor: pbrpc.WebSocketAcceptor{}, Channel: pbrpc.ServerChannelPolicy{ChannelPolicy: pbrpc.ChannelPolicy{Logger: *(&logger.Logger{}).Initialize("pbrpc-srv1", logger.SeverityInfo, os.Stdout, os.Stderr)}}}).RegisterServiceHandler(serviceHandler)
	server := (&pbrpc.Server{}).Initialize(serverPolicy, "127.0.0.1:8888", "", context.Background())
	server.Run()
}
