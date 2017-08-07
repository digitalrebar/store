package store

// MemoryStore provides an in-memory implementation of Store
// for testing purposes
type Memory struct {
	storeBase
	v map[string][]byte
}

func (m *Memory) Open(codec Codec) error {
	if codec == nil {
		codec = DefaultCodec
	}
	m.Codec = codec
	m.closer = func() {
		m.v = nil
	}
	m.v = map[string][]byte{}
	m.opened = true
	return nil
}

func (m *Memory) MakeSub(loc string) (Store, error) {
	m.Lock()
	defer m.Unlock()
	m.panicIfClosed()
	res := &Memory{}
	res.Open(m.Codec)
	addSub(m, res, loc)
	return res, nil
}

func (m *Memory) Keys() ([]string, error) {
	m.RLock()
	m.panicIfClosed()
	res := make([]string, 0, len(m.v))
	for k := range m.v {
		res = append(res, k)
	}
	m.RUnlock()
	return res, nil
}

func (m *Memory) Load(key string, val interface{}) error {
	m.RLock()
	m.panicIfClosed()
	v, ok := m.v[key]
	m.RUnlock()
	if !ok {
		return NotFound(key)
	}
	return m.Decode(v, val)
}

func (m *Memory) Save(key string, val interface{}) error {
	m.Lock()
	defer m.Unlock()
	m.panicIfClosed()
	if m.readOnly {
		return UnWritable(key)
	}
	buf, err := m.Encode(val)
	if err != nil {
		return err
	}
	m.v[key] = buf
	return nil
}

func (m *Memory) Remove(key string) error {
	m.Lock()
	defer m.Unlock()
	m.panicIfClosed()
	_, ok := m.v[key]
	if ok {
		if m.readOnly {
			return UnWritable(key)
		}
		delete(m.v, key)
		return nil
	}
	return NotFound(key)
}
