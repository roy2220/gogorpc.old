package channel

import (
	"context"
	"sync"

	"github.com/let-z-go/pbrpc/internal/stream"
)

type pendingRPC struct {
	IsEmitted       bool
	OutputExtraData ExtraData
	Response        stream.Message
	Err             error

	responseFactory MessageFactory
	completion      chan struct{}
}

func (self *pendingRPC) Init(responseFactory MessageFactory) *pendingRPC {
	self.responseFactory = responseFactory
	self.completion = make(chan struct{})
	return self
}

func (self *pendingRPC) NewResponse() stream.Message {
	return self.responseFactory()
}

func (self *pendingRPC) Succeed(outputExtraData ExtraData, response stream.Message) {
	self.OutputExtraData = outputExtraData
	self.Response = response
	close(self.completion)
}

func (self *pendingRPC) Fail(outputExtraData ExtraData, err error) {
	self.OutputExtraData = outputExtraData
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
