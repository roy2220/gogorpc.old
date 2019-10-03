package channel

import (
	"context"
	"sync"
)

type inflightRPC struct {
	IsEmitted        bool
	ResponseMetadata Metadata
	Response         Message
	Err              error

	responseFactory MessageFactory
	completion      chan struct{}
}

func (self *inflightRPC) Init(responseFactory MessageFactory) {
	self.responseFactory = responseFactory
	self.completion = make(chan struct{})
}

func (self *inflightRPC) NewResponse() Message {
	return self.responseFactory()
}

func (self *inflightRPC) Succeed(metadata Metadata, response Message) {
	self.ResponseMetadata = metadata
	self.Response = response
	close(self.completion)
}

func (self *inflightRPC) Fail(metadata Metadata, err error) {
	self.ResponseMetadata = metadata
	self.Response = NullMessage
	self.Err = err
	close(self.completion)
}

func (self *inflightRPC) WaitFor(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-self.completion:
		return nil
	}
}

var inflightRPCPool = sync.Pool{}

func newPooledInflightRPC() *inflightRPC {
	var inflightRPC_ *inflightRPC

	if value := inflightRPCPool.Get(); value == nil {
		inflightRPC_ = new(inflightRPC)
	} else {
		inflightRPC_ = value.(*inflightRPC)
		*inflightRPC_ = inflightRPC{}
	}

	return inflightRPC_
}

func putPooledInflightRPC(inflightRPC_ *inflightRPC) {
	inflightRPCPool.Put(inflightRPC_)
}
