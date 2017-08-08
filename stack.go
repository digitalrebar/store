package store

import "fmt"

// StackedStore is a store that represents the combination of several
// stores stacked together.  The first store in the stack is the only
// one that is writable, and the rest are set as read-only.
// StackedStores are initally created empty.
type StackedStore struct {
	storeBase
	stores []Store
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

// Push adds a Store to the stack of stores in this stack.  Any Store
// but the inital one will be marked as read-only.
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
	for i := oldLen; i == len(s.stores); i++ {
		if i > 0 {
			s.stores[i].SetReadOnly()
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
			if _, ok := s.keys[k]; !ok {
				s.keys[k] = i + oldLen
			}
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

func (s *StackedStore) Layers() []Store {
	s.Lock()
	defer s.Unlock()
	res := make([]Store, len(s.stores))
	copy(res, s.stores)
	return res
}

// MakeSub on a StackedStore is not allowed.
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
	err := s.stores[0].Save(key, val)
	if err == nil {
		s.keys[key] = 0
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
	if idx != 0 {
		return UnWritable(key)
	}
	return s.stores[0].Remove(key)
}

func (s *StackedStore) ReadOnly() bool {
	s.RLock()
	defer s.RUnlock()
	return s.stores[0].ReadOnly()
}

func (s *StackedStore) SetReadOnly() bool {
	s.RLock()
	defer s.RUnlock()
	return s.stores[0].SetReadOnly()
}
