package store

import (
	"fmt"
	"sync"
)

type StackedStore struct {
	sync.RWMutex
	stores []SimpleStore
	top    SimpleStore
	keys   map[string]int
}

func (s *StackedStore) Push(stores ...SimpleStore) error {
	s.Lock()
	var scanIdx int
	if s.stores == nil {
		s.stores = stores
	} else {
		scanIdx = len(s.stores)
		s.stores = append(s.stores, stores...)
	}
	s.top = s.stores[len(s.stores)-1]
	if len(s.stores) > 1 {
		for i := len(s.stores) - 2; i >= 0; i-- {
			s.stores[i].SetReadOnly()
		}
	}
	for i := scanIdx; i < len(s.stores); i++ {
		newKeys, err := s.stores[i].Keys()
		if err != nil {
			return err
		}
		for _, k := range newKeys {
			s.keys[k] = i
		}
	}
	s.Unlock()
	return nil
}

func (s *StackedStore) Sub(st string) (SimpleStore, error) {
	return nil, fmt.Errorf("Cannot create substore %s on a stacked store", st)
}

func (s *StackedStore) Keys() ([]string, error) {
	s.RLock()
	defer s.RUnlock()
	vals := make([]string, 0, len(s.keys))
	for k := range s.keys {
		vals = append(vals, k)
	}
	return vals, nil
}

func (s *StackedStore) Load(key string, val interface{}) error {
	s.RLock()
	defer s.RUnlock()
	idx, ok := s.keys[key]
	if !ok {
		return NotFound(key)
	}
	return s.stores[idx].Load(key, val)
}

func (s *StackedStore) Save(key string, val interface{}) error {
	s.RLock()
	defer s.RUnlock()
	err := s.top.Save(key, val)
	if err == nil {
		s.keys[key] = len(s.stores) - 1
	}
	return err
}

func (s *StackedStore) Remove(key string) error {
	s.RLock()
	defer s.RUnlock()
	idx, ok := s.keys[key]
	if !ok {
		return NotFound(key)
	}
	if idx != len(s.stores)-1 {
		return UnWritable(key)
	}
	return s.top.Remove(key)
}

func (s *StackedStore) Encode(i interface{}) ([]byte, error) {
	return nil, fmt.Errorf("Do not call Encode() on a stacked store directly")
}

func (s *StackedStore) Decode(buf []byte, i interface{}) error {
	return fmt.Errorf("Do not call Decode() on a stacked store directly")
}

func (s *StackedStore) ReadOnly() bool {
	s.RLock()
	defer s.RUnlock()
	return s.top.ReadOnly()
}

func (s *StackedStore) SetReadOnly() bool {
	s.RLock()
	defer s.RUnlock()
	return s.top.SetReadOnly()
}
