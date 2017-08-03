package store

import "sync"

// MemoryStore provides an in-memory implementation of SimpleStore
// for testing purposes
type SimpleMemoryStore struct {
	Codec
	ro
	sync.RWMutex
	v map[string][]byte
}

func NewSimpleMemoryStore(codec Codec) *SimpleMemoryStore {
	if codec == nil {
		codec = DefaultCodec
	}
	return &SimpleMemoryStore{Codec: codec, v: make(map[string][]byte)}
}

func (m *SimpleMemoryStore) Sub(loc string) (SimpleStore, error) {
	return NewSimpleMemoryStore(m.Codec), nil
}

func (m *SimpleMemoryStore) Keys() ([]string, error) {
	m.RLock()
	res := make([]string, 0, len(m.v))
	for k := range m.v {
		res = append(res, k)
	}
	m.RUnlock()
	return res, nil
}

func (m *SimpleMemoryStore) Load(key string, val interface{}) error {
	m.RLock()
	v, ok := m.v[key]
	m.RUnlock()
	if !ok {
		return NotFound(key)
	}
	return m.Decode(v, val)
}

func (m *SimpleMemoryStore) Save(key string, val interface{}) error {
	m.Lock()
	defer m.Unlock()
	if m.ReadOnly() {
		return UnWritable(key)
	}
	buf, err := m.Encode(val)
	if err != nil {
		return err
	}
	m.v[key] = buf
	return nil
}

func (m *SimpleMemoryStore) Remove(key string) error {
	m.Lock()
	defer m.Unlock()
	_, ok := m.v[key]
	if ok {
		if m.ReadOnly() {
			return UnWritable(key)
		}
		delete(m.v, key)
		return nil
	}
	return NotFound(key)
}
