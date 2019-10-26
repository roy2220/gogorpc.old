package stream

import (
	"fmt"
)

type ExtraData map[string][]byte

func (ed ExtraData) TryGet(key string) ([]byte, bool) {
	value, ok := ed[key]
	return value, ok
}

func (ed ExtraData) Get(key string, defaultValue []byte) []byte {
	value, ok := ed[key]

	if !ok {
		value = defaultValue
	}

	return value
}

func (ed ExtraData) MustGet(key string) []byte {
	value, ok := ed[key]

	if !ok {
		panic(fmt.Errorf("gogorpc/stream: extra data key not found: key=%#v", key))
	}

	return value
}

func (ed *ExtraData) Set(key string, value []byte) {
	if *ed == nil {
		*ed = make(ExtraData, 1)
	}

	(*ed)[key] = value
}

func (ed ExtraData) Clear(key string) {
	delete(ed, key)
}

func (ed ExtraData) Copy() ExtraData {
	if ed == nil {
		return nil
	}

	copy_ := make(ExtraData, len(ed))

	for key, value := range ed {
		copy_[key] = value
	}

	return copy_
}

func (ed ExtraData) Ref(copyOnWrite bool) ExtraDataRef {
	return ExtraDataRef{
		value:       ed,
		copyOnWrite: copyOnWrite,
	}
}

type ExtraDataRef struct {
	value       ExtraData
	copyOnWrite bool
}

func (edr ExtraDataRef) TryGet(key string) ([]byte, bool) {
	return edr.value.TryGet(key)
}

func (edr ExtraDataRef) Get(key string, defaultValue []byte) []byte {
	return edr.value.Get(key, defaultValue)
}

func (edr ExtraDataRef) MustGet(key string) []byte {
	return edr.value.MustGet(key)
}

func (edr *ExtraDataRef) Set(key string, value []byte) {
	edr.preWrite()
	edr.value.Set(key, value)
}

func (edr *ExtraDataRef) Clear(key string) {
	edr.preWrite()
	edr.value.Clear(key)
}

func (edr ExtraDataRef) Value() ExtraData {
	return edr.value
}

func (edr ExtraDataRef) preWrite() {
	if edr.copyOnWrite {
		edr.value = edr.value.Copy()
		edr.copyOnWrite = false
	}
}
