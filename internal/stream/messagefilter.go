package stream

type MessageFilter interface {
	// Input:
	//   packet.Message
	// Output:
	//   packet.Err
	FilterIncomingKeepalive(packet *Packet)
	FilterOutgoingKeepalive(packet *Packet)

	// Input:
	//   packet.RequestHeader
	//   packet.Message
	// Output:
	//   packet.Err
	FilterIncomingRequest(packet *Packet)
	FilterOutgoingRequest(packet *Packet)

	// Input:
	//   packet.ResponseHeader
	//   packet.Message
	// Output:
	//   packet.Err
	FilterIncomingResponse(packet *Packet)
	FilterOutgoingResponse(packet *Packet)

	// Input:
	//   packet.Hangup
	// Output:
	//   packet.Err
	FilterIncomingHangup(packet *Packet)
	FilterOutgoingHangup(packet *Packet)
}

type IncomingMessageFilter interface {
	FilterIncomingKeepalive(packet *Packet)
	FilterIncomingRequest(packet *Packet)
	FilterIncomingResponse(packet *Packet)
	FilterIncomingHangup(packet *Packet)
}

type OutgoingMessageFilter interface {
	FilterOutgoingKeepalive(packet *Packet)
	FilterOutgoingRequest(packet *Packet)
	FilterOutgoingResponse(packet *Packet)
	FilterOutgoingHangup(packet *Packet)
}

var _ = (MessageFilter)(interface {
	IncomingMessageFilter
	OutgoingMessageFilter
}(nil))

type KeepaliveFilter interface {
	FilterIncomingKeepalive(packet *Packet)
	FilterOutgoingKeepalive(packet *Packet)
}

type RequestFilter interface {
	FilterIncomingRequest(packet *Packet)
	FilterOutgoingRequest(packet *Packet)
}

type ResponseFilter interface {
	FilterIncomingResponse(packet *Packet)
	FilterOutgoingResponse(packet *Packet)
}

type HangupFilter interface {
	FilterIncomingHangup(packet *Packet)
	FilterOutgoingHangup(packet *Packet)
}

var _ = (MessageFilter)(interface {
	KeepaliveFilter
	RequestFilter
	ResponseFilter
	HangupFilter
}(nil))

type DummyMessageFilter struct {
	DummyIncomingMessageFilter
	DummyOutgoingMessageFilter
	// DummyKeepaliveFilter
	// DummyRequestFilter
	// DummyResponseFilter
	// DummyHangupFilter
}

var _ = MessageFilter(DummyMessageFilter{})

type DummyIncomingMessageFilter struct{}

var _ = IncomingMessageFilter(DummyIncomingMessageFilter{})

func (DummyIncomingMessageFilter) FilterIncomingKeepalive(*Packet) {}
func (DummyIncomingMessageFilter) FilterIncomingRequest(*Packet)   {}
func (DummyIncomingMessageFilter) FilterIncomingResponse(*Packet)  {}
func (DummyIncomingMessageFilter) FilterIncomingHangup(*Packet)    {}

type DummyOutgoingMessageFilter struct{}

var _ = DummyOutgoingMessageFilter(DummyOutgoingMessageFilter{})

func (DummyOutgoingMessageFilter) FilterOutgoingKeepalive(*Packet) {}
func (DummyOutgoingMessageFilter) FilterOutgoingRequest(*Packet)   {}
func (DummyOutgoingMessageFilter) FilterOutgoingResponse(*Packet)  {}
func (DummyOutgoingMessageFilter) FilterOutgoingHangup(*Packet)    {}

type DummyKeepaliveFilter struct{}

var _ = KeepaliveFilter(DummyKeepaliveFilter{})

func (DummyKeepaliveFilter) FilterIncomingKeepalive(*Packet) {}
func (DummyKeepaliveFilter) FilterOutgoingKeepalive(*Packet) {}

type DummyRequestFilter struct{}

var _ = RequestFilter(DummyRequestFilter{})

func (DummyRequestFilter) FilterIncomingRequest(*Packet) {}
func (DummyRequestFilter) FilterOutgoingRequest(*Packet) {}

type DummyResponseFilter struct{}

var _ = ResponseFilter(DummyResponseFilter{})

func (DummyResponseFilter) FilterIncomingResponse(*Packet) {}
func (DummyResponseFilter) FilterOutgoingResponse(*Packet) {}

type DummyHangupFilter struct{}

var _ = HangupFilter(DummyHangupFilter{})

func (DummyHangupFilter) FilterIncomingHangup(*Packet) {}
func (DummyHangupFilter) FilterOutgoingHangup(*Packet) {}
