package stream

import (
	"fmt"
)

type ExtraData map[string][]byte

func (self ExtraData) TryGet(key string) ([]byte, bool) {
	value, ok := self[key]
	return value, ok
}

func (self ExtraData) Get(key string, defaultValue []byte) []byte {
	value, ok := self[key]

	if !ok {
		value = defaultValue
	}

	return value
}

func (self ExtraData) MustGet(key string) []byte {
	value, ok := self[key]

	if !ok {
		panic(fmt.Errorf("gogorpc/stream: extra data key not found: key=%#v", key))
	}

	return value
}

func (self *ExtraData) Set(key string, value []byte) {
	if *self == nil {
		*self = make(ExtraData, 1)
	}

	(*self)[key] = value
}

func (self ExtraData) Clear(key string) {
	delete(self, key)
}

func (self ExtraData) Copy() ExtraData {
	if self == nil {
		return nil
	}

	copy_ := make(ExtraData, len(self))

	for key, value := range self {
		copy_[key] = value
	}

	return copy_
}

func (self ExtraData) Ref(copyOnWrite bool) ExtraDataRef {
	return ExtraDataRef{
		value:       self,
		copyOnWrite: copyOnWrite,
	}
}

type ExtraDataRef struct {
	value       ExtraData
	copyOnWrite bool
}

func (self ExtraDataRef) TryGet(key string) ([]byte, bool) {
	return self.value.TryGet(key)
}

func (self ExtraDataRef) Get(key string, defaultValue []byte) []byte {
	return self.value.Get(key, defaultValue)
}

func (self ExtraDataRef) MustGet(key string) []byte {
	return self.value.MustGet(key)
}

func (self *ExtraDataRef) Set(key string, value []byte) {
	self.preWrite()
	self.value.Set(key, value)
}

func (self *ExtraDataRef) Clear(key string) {
	self.preWrite()
	self.value.Clear(key)
}

func (self ExtraDataRef) Value() ExtraData {
	return self.value
}

func (self ExtraDataRef) preWrite() {
	if self.copyOnWrite {
		self.value = self.value.Copy()
		self.copyOnWrite = false
	}
}
