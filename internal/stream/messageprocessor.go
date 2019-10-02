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
	//   packet.Message
	//   packet.Err
	NewKeepalive(packet *Packet)

	// Input:
	//   packet.RequestHeader
	// Output:
	//   packet.Message
	//   packet.Err
	NewRequest(packet *Packet)

	// Input:
	//   packet.ResponseHeader
	// Output:
	//   packet.Message
	//   packet.Err
	NewResponse(packet *Packet)
}

type MessageHandler interface {
	// Input:
	//   packet.KeepaliveHeader
	//   packet.Message
	//   packet.Err
	// Output:
	//   packet.Err
	HandleKeepalive(ctx context.Context, packet *Packet)

	// Input:
	//   packet.RequestHeader
	//   packet.Message
	//   packet.Err
	// Output:
	//   packet.Err
	HandleRequest(ctx context.Context, packet *Packet)

	// Input:
	//   packet.ResponseHeader
	//   packet.Message
	//   packet.Err
	// Output:
	//   packet.Err
	HandleResponse(ctx context.Context, packet *Packet)
}

type MessageEmitter interface {
	// Output:
	//   packet.Message
	//   packet.Err
	EmitKeepalive(packet *Packet)

	// Input:
	//   packet.RequestHeader
	//   packet.Message
	//   packet.Err
	// Output:
	//   packet.RequestHeader
	//   packet.Message
	//   packet.Err
	PostEmitRequest(packet *Packet)

	// Input:
	//   packet.ResponseHeader
	//   packet.Message
	//   packet.Err
	// Output:
	//   packet.ResponseHeader
	//   packet.Message
	//   packet.Err
	PostEmitResponse(packet *Packet)
}
