package stream

type Metadata map[string][]byte

func (self *Metadata) Set(key string, value []byte) {
	if *self == nil {
		*self = map[string][]byte{}
	}

	(*self)[key] = value
}

func (self Metadata) Clear(key string) {
	delete(self, key)
}

func (self Metadata) Get(key string) ([]byte, bool) {
	value, ok := self[key]
	return value, ok
}
