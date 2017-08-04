package store

import (
	"fmt"
	"sync"
)

// SimpleStore provides an interface for some very basic key/value
// storage needs.  Each SimpleStore (including ones created with MakeSub()
// should operate as seperate, flat key/value stores.
type SimpleStore interface {
	// GetSub fetches an already-existing substore.  nil means there is no such substore.
	GetSub(string) SimpleStore
	// MakeSub returns a SimpleStore that is subordinate to this one.
	// What exactly that means depends on the simplestore in question,
	// but it should wind up sharing the same backing store (directory,
	// database, etcd cluster, whatever)
	MakeSub(string) (SimpleStore, error)
	// Parent fetches the parent of this store, if any.
	Parent() SimpleStore
	// Keys returns the list of keys that this store has in no
	// particular order.
	Keys() ([]string, error)
	// Subs returns a map all of the substores for this store.
	Subs() map[string]SimpleStore
	// Load the data for a particular key
	Load(string, interface{}) error
	// Save data for a key
	Save(string, interface{}) error
	// Remove a key/value pair.
	Remove(string) error
	// Control the writeability of the store
	ReadOnly() bool
	SetReadOnly() bool
	// Stop the store from further operations.
	Close()
}

type parentSetter interface {
	setParent(SimpleStore)
}

type childSetter interface {
	addChild(string, SimpleStore)
}

type forceCloser interface {
	forceClose()
}

// NotFound is the "key not found" error type.
type NotFound string

func (n NotFound) Error() string {
	return fmt.Sprintf("key %s: not found", string(n))
}

type UnWritable string

func (u UnWritable) Error() string {
	return fmt.Sprintf("readonly: %s", string(u))
}

type storeBase struct {
	sync.RWMutex
	Codec
	readOnly    bool
	closed      bool
	storeType   string
	subStores   map[string]SimpleStore
	parentStore SimpleStore
	closer      func()
}

func (s *storeBase) forceClose() {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return
	}
	if s.closer != nil {
		s.closer()
	}
	s.closed = true
}

func (s *storeBase) Close() {
	s.Lock()
	if s.parentStore == nil {
		s.Unlock()
		s.forceClose()
		for _, sub := range s.subStores {
			sub.(forceCloser).forceClose()
		}
		return
	}
	parent := s.parentStore
	s.Unlock()
	parent.Close()
	return
}

func (s *storeBase) panicIfClosed() {
	if s.closed {
		panic("Operation on closed store")
	}
}

func (s *storeBase) ReadOnly() bool {
	s.RLock()
	defer s.RUnlock()
	s.panicIfClosed()
	return s.readOnly
}

func (s *storeBase) SetReadOnly() bool {
	s.Lock()
	defer s.Unlock()
	s.panicIfClosed()
	if s.readOnly {
		return false
	}
	s.readOnly = true
	for _, sub := range s.subStores {
		sub.SetReadOnly()
	}
	return true
}

func (s *storeBase) GetSub(name string) SimpleStore {
	s.RLock()
	defer s.RUnlock()
	s.panicIfClosed()
	if s.subStores == nil {
		return nil
	}
	return s.subStores[name]
}

func (s *storeBase) Subs() map[string]SimpleStore {
	s.RLock()
	defer s.RUnlock()
	s.panicIfClosed()
	res := map[string]SimpleStore{}
	for k, v := range s.subStores {
		res[k] = v
	}
	return res
}

func (s *storeBase) Parent() SimpleStore {
	s.RLock()
	defer s.RUnlock()
	s.panicIfClosed()
	return s.parentStore.(SimpleStore)
}

func (s *storeBase) setParent(p SimpleStore) {
	s.parentStore = p
}

func (s *storeBase) addChild(name string, c SimpleStore) {
	if s.subStores == nil {
		s.subStores = map[string]SimpleStore{}
	}
	s.subStores[name] = c
}

func addSub(parent, child SimpleStore, name string) {
	parent.(childSetter).addChild(name, child)
	child.(parentSetter).setParent(parent)
}
