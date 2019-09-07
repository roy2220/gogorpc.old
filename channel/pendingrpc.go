package channel

import (
	"context"
	"sync"
)

type pendingRPC struct {
	IsEmitted        bool
	ResponseMetadata Metadata
	Response         Message
	Err              error

	responseFactory MessageFactory
	completion      chan struct{}
}

func (self *pendingRPC) Init(responseFactory MessageFactory) {
	self.responseFactory = responseFactory
	self.completion = make(chan struct{})
}

func (self *pendingRPC) NewResponse() Message {
	return self.responseFactory()
}

func (self *pendingRPC) Succeed(metadata Metadata, response Message) {
	self.ResponseMetadata = metadata
	self.Response = response
	close(self.completion)
}

func (self *pendingRPC) Fail(metadata Metadata, err error) {
	self.ResponseMetadata = metadata
	self.Response = NullMessage
	self.Err = err
	close(self.completion)
}

func (self *pendingRPC) WaitFor(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-self.completion:
		return nil
	}
}

var pendingRPCPool = sync.Pool{New: func() interface{} { return new(pendingRPC) }}

func getPooledPendingRPC() *pendingRPC {
	pendingRPC_ := pendingRPCPool.Get().(*pendingRPC)
	*pendingRPC_ = pendingRPC{}
	return pendingRPC_
}

func putPooledPendingRPC(pendingRPC_ *pendingRPC) {
	pendingRPCPool.Put(pendingRPC_)
}
