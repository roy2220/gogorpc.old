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

func (ServerServiceHandler) SayHello(context_ context.Context, request *sample.SayHelloRequest) (*sample.SayHelloResponse, error) {
	contextVars := pbrpc.MustGetContextVars(context_)
	client := sample.MakeClientServiceClient(contextVars.Channel).WithAutoRetry(true)
	response, _ := client.GetNickname(context_)

	response2 := &sample.SayHelloResponse{
		Reply: fmt.Sprintf(request.ReplyFormat, response.Nickname),
	}

	return response2, nil
}

func InterceptIncomingMethod(context_ context.Context, request interface{}, incomingMethodDispatcher pbrpc.IncomingMethodDispatcher) (pbrpc.OutgoingMessage, error) {
	contextVars := pbrpc.MustGetContextVars(context_)
	fmt.Printf("%v.%v begin\n", contextVars.ServiceName, contextVars.MethodName)
	fmt.Printf("trace_id=%#v, spanParentID=%#v, spanID=%#v\n", contextVars.TraceID.String(), contextVars.SpanParentID, contextVars.SpanID)
	fmt.Printf("request=%#v\n", request)
	response, e := incomingMethodDispatcher(context_, request)
	fmt.Printf("response=%#v\n", response)
	fmt.Printf("%v.%v end\n", contextVars.ServiceName, contextVars.MethodName)
	return response, e
}

func main() {
	serviceHandler := ServerServiceHandler{}

	serverPolicy := pbrpc.ServerPolicy{
		Channel: &pbrpc.ServerChannelPolicy{
			ChannelPolicy: (&pbrpc.ChannelPolicy{}).
				RegisterServiceHandler(serviceHandler).
				AddIncomingMethodInterceptor("", -1, InterceptIncomingMethod),
		},
	}

	server := (&pbrpc.Server{}).Initialize(&serverPolicy, "127.0.0.1:8888", "", context.Background())
	panic(server.Run())
}
