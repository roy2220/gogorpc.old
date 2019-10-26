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

func (ir *inflightRPC) Init(responseFactory MessageFactory) *inflightRPC {
	ir.responseFactory = responseFactory
	ir.completion = make(chan struct{}, 1)
	return ir
}

func (ir *inflightRPC) Reset(responseFactory MessageFactory) *inflightRPC {
	ir.IsEmitted = false
	ir.ResponseExtraData = nil
	ir.Response = nil
	ir.Err = nil
	ir.responseFactory = responseFactory
	return ir
}

func (ir *inflightRPC) NewResponse() Message {
	return ir.responseFactory()
}

func (ir *inflightRPC) Succeed(extraData ExtraData, response Message) {
	ir.ResponseExtraData = extraData
	ir.Response = response
	ir.completion <- struct{}{}
}

func (ir *inflightRPC) Fail(extraData ExtraData, err error) {
	ir.ResponseExtraData = extraData
	ir.Response = NullMessage
	ir.Err = err
	ir.completion <- struct{}{}
}

func (ir *inflightRPC) WaitFor(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ir.completion:
		return nil
	}
}

var inflightRPCPool = sync.Pool{}

func getPooledInflightRPC(responseFactory MessageFactory) *inflightRPC {
	if value := inflightRPCPool.Get(); value != nil {
		return value.(*inflightRPC).Reset(responseFactory)
	}

	return new(inflightRPC).Init(responseFactory)
}

func putPooledInflightRPC(inflightRPC_ *inflightRPC) {
	inflightRPCPool.Put(inflightRPC_)
}
