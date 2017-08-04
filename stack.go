package store

import "fmt"

type StackedStore struct {
	storeBase
	stores []SimpleStore
	top    SimpleStore
	keys   map[string]int
}

func NewStackedStore(stores ...SimpleStore) (*StackedStore, error) {
	if stores == nil || len(stores) == 0 {
		return nil, fmt.Errorf("Stacked store must include a list of stores to stack")
	}
	res := &StackedStore{}
	res.stores = stores
	res.top = stores[len(stores)-1]
	if len(stores) > 1 {
		for i := len(stores) - 2; i >= 0; i-- {
			stores[i].SetReadOnly()
		}
	}
	subStacks := map[string][]SimpleStore{}
	for i, item := range stores {
		newKeys, err := item.Keys()
		if err != nil {
			return nil, err
		}
		for _, k := range newKeys {
			res.keys[k] = i
		}
		for k, v := range item.Subs() {
			if _, ok := subStacks[k]; !ok {
				subStacks[k] = []SimpleStore{v}
			} else {
				subStacks[k] = append(subStacks[k], v)
			}
		}
	}
	for k, v := range subStacks {
		sub, err := NewStackedStore(v...)
		if err != nil {
			return nil, err
		}
		addSub(res, sub, k)
	}
	return res, nil
}

func (s *StackedStore) MakeSub(st string) (SimpleStore, error) {
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
