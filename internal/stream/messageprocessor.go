package stream

import (
	"context"
)

type MessageProcessor interface {
	MessageFactory
	MessageHandler
	MessageEmitter
}

type MessageFactory interface {
	// Output:
	//   event.Message
	//   event.Err
	NewKeepalive(event *Event)

	// Input:
	//   event.RequestHeader
	// Output:
	//   event.Message
	//   event.Err
	NewRequest(event *Event)

	// Input:
	//   event.ResponseHeader
	// Output:
	//   event.Message
	//   event.Err
	NewResponse(event *Event)
}

type MessageHandler interface {
	// Input:
	//   event.KeepaliveHeader
	//   event.Message
	//   event.Err
	// Output:
	//   event.Err
	HandleKeepalive(ctx context.Context, event *Event)

	// Input:
	//   event.RequestHeader
	//   event.Message
	//   event.Err
	// Output:
	//   event.Err
	HandleRequest(ctx context.Context, event *Event)

	// Input:
	//   event.ResponseHeader
	//   event.Message
	//   event.Err
	// Output:
	//   event.Err
	HandleResponse(ctx context.Context, event *Event)
}

type MessageEmitter interface {
	// Output:
	//   event.Message
	//   event.Err
	EmitKeepalive(event *Event)

	// Input:
	//   event.RequestHeader
	//   event.Message
	//   event.Err
	// Output:
	//   event.RequestHeader
	//   event.Message
	//   event.Err
	PostEmitRequest(event *Event)

	// Input:
	//   event.ResponseHeader
	//   event.Message
	//   event.Err
	// Output:
	//   event.ResponseHeader
	//   event.Message
	//   event.Err
	PostEmitResponse(event *Event)
}
