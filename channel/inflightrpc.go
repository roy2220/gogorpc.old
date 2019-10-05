package channel

import (
	"context"
	"sync"
)

type inflightRPC struct {
	IsEmitted         bool
	ResponseExtraData ExtraData
	Response          Message
	Err               error

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

func (self *inflightRPC) Succeed(extraData ExtraData, response Message) {
	self.ResponseExtraData = extraData
	self.Response = response
	close(self.completion)
}

func (self *inflightRPC) Fail(extraData ExtraData, err error) {
	self.ResponseExtraData = extraData
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
