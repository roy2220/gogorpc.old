package channel

import (
	"sync"
	"unsafe"
)

func (r *RPC) DetachFromPool() *RPC {
	r.pooled().isDetached = true
	return r
}

func (r *RPC) pooled() *pooledRPC {
	return (*pooledRPC)(unsafe.Pointer(uintptr(unsafe.Pointer(r)) - unsafe.Offsetof(pooledRPC{}.RPC)))
}

func GetPooledRPC() *RPC {
	pooledRPC_ := rpcPool.Get().(*pooledRPC)
	return &pooledRPC_.RPC
}

func PutPooledRPC(rpc *RPC) {
	pooledRPC_ := rpc.pooled()

	if pooledRPC_.isDetached {
		return
	}

	rpcPool.Put(pooledRPC_)
}

type pooledRPC struct {
	RPC

	isDetached bool
}

var rpcPool = sync.Pool{New: func() interface{} { return new(pooledRPC) }}
