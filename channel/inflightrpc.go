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

func (self *inflightRPC) Init(responseFactory MessageFactory) *inflightRPC {
	self.responseFactory = responseFactory
	self.completion = make(chan struct{}, 1)
	return self
}

func (self *inflightRPC) Reset(responseFactory MessageFactory) *inflightRPC {
	self.IsEmitted = false
	self.ResponseExtraData = nil
	self.Response = nil
	self.Err = nil
	self.responseFactory = responseFactory
	return self
}

func (self *inflightRPC) NewResponse() Message {
	return self.responseFactory()
}

func (self *inflightRPC) Succeed(extraData ExtraData, response Message) {
	self.ResponseExtraData = extraData
	self.Response = response
	self.completion <- struct{}{}
}

func (self *inflightRPC) Fail(extraData ExtraData, err error) {
	self.ResponseExtraData = extraData
	self.Response = NullMessage
	self.Err = err
	self.completion <- struct{}{}
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

func getPooledInflightRPC(responseFactory MessageFactory) *inflightRPC {
	var inflightRPC_ *inflightRPC

	if value := inflightRPCPool.Get(); value == nil {
		inflightRPC_ = new(inflightRPC).Init(responseFactory)
	} else {
		inflightRPC_ = value.(*inflightRPC).Reset(responseFactory)
	}

	return inflightRPC_
}

func putPooledInflightRPC(inflightRPC_ *inflightRPC) {
	inflightRPCPool.Put(inflightRPC_)
}
