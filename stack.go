package store

import "fmt"

type StackedStore struct {
	storeBase
	stores []Store
	top    Store
	keys   map[string]int
}

func (s *StackedStore) Open(codec Codec) error {
	s.Codec = codec
	s.stores = []Store{}
	s.keys = map[string]int{}
	s.opened = true
	s.closer = func() {
		for _, item := range s.stores {
			item.Close()
		}
	}
	return nil
}

func (s *StackedStore) Push(stores ...Store) error {
	if len(stores) == 0 {
		return nil
	}
	s.Lock()
	defer s.Unlock()
	s.panicIfClosed()
	oldLen := len(s.stores)
	s.stores = append(s.stores, stores...)
	// Cache the top store for quick access
	s.top = stores[len(stores)-1]
	if len(stores) > 1 {
		for i := len(stores) - 2; i >= 0; i-- {
			stores[i].SetReadOnly()
		}
	}
	// Update the key mappings
	subStacks := map[string][]Store{}
	for i, item := range stores {
		newKeys, err := item.Keys()
		if err != nil {
			return err
		}
		for _, k := range newKeys {
			s.keys[k] = i + oldLen
		}
		for k, v := range item.Subs() {
			if _, ok := subStacks[k]; !ok {
				subStacks[k] = []Store{v}
			} else {
				subStacks[k] = append(subStacks[k], v)
			}
		}
	}
	// Update or create new subs as needed.
	for k, v := range subStacks {
		sub := s.subStores[k].(*StackedStore)
		if sub == nil {
			sub = &StackedStore{}
			sub.Open(s.Codec)
		}
		if err := sub.Push(v...); err != nil {
			return err
		}
		sub.closer = nil
		addSub(s, sub, k)
	}
	return nil
}

func (s *StackedStore) MakeSub(st string) (Store, error) {
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
