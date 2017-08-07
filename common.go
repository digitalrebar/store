package store

import (
	"fmt"
	"sync"
)

// Store provides an interface for some very basic key/value
// storage needs.  Each Store (including ones created with MakeSub()
// should operate as seperate, flat key/value stores.
type Store interface {
	// Open opens the store for use.
	Open(Codec) error
	// GetSub fetches an already-existing substore.  nil means there is no such substore.
	GetSub(string) Store
	// MakeSub returns a Store that is subordinate to this one.
	// What exactly that means depends on the simplestore in question,
	// but it should wind up sharing the same backing store (directory,
	// database, etcd cluster, whatever)
	MakeSub(string) (Store, error)
	// Parent fetches the parent of this store, if any.
	Parent() Store
	// Keys returns the list of keys that this store has in no
	// particular order.
	Keys() ([]string, error)
	// Subs returns a map all of the substores for this store.
	Subs() map[string]Store
	// Load the data for a particular key
	Load(string, interface{}) error
	// Save data for a key
	Save(string, interface{}) error
	// Remove a key/value pair.
	Remove(string) error
	// ReadOnly returns whether a store is set to be read-only.
	ReadOnly() bool
	// SetReadOnly sets the store into read-only mode.  This is a
	// one-way operation -- once a store is set to read-only, it
	// cannot be changed back to read-write while the store is open.
	SetReadOnly() bool
	// Close closes the store.  Attempting to perfrom operations on
	// a closed store will panic.
	Close()
}

type parentSetter interface {
	setParent(Store)
}

type childSetter interface {
	addChild(string, Store)
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
	opened      bool
	storeType   string
	subStores   map[string]Store
	parentStore Store
	closer      func()
}

func (s *storeBase) forceClose() {
	s.Lock()
	defer s.Unlock()
	if !s.opened {
		return
	}
	if s.closer != nil {
		s.closer()
	}
	s.opened = false
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
	if !s.opened {
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

func (s *storeBase) GetSub(name string) Store {
	s.RLock()
	defer s.RUnlock()
	s.panicIfClosed()
	if s.subStores == nil {
		return nil
	}
	return s.subStores[name]
}

func (s *storeBase) Subs() map[string]Store {
	s.RLock()
	defer s.RUnlock()
	s.panicIfClosed()
	res := map[string]Store{}
	for k, v := range s.subStores {
		res[k] = v
	}
	return res
}

func (s *storeBase) Parent() Store {
	s.RLock()
	defer s.RUnlock()
	s.panicIfClosed()
	return s.parentStore.(Store)
}

func (s *storeBase) setParent(p Store) {
	s.parentStore = p
}

func (s *storeBase) addChild(name string, c Store) {
	if s.subStores == nil {
		s.subStores = map[string]Store{}
	}
	s.subStores[name] = c
}

func addSub(parent, child Store, name string) {
	parent.(childSetter).addChild(name, child)
	child.(parentSetter).setParent(parent)
}
