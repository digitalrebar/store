package store

import "fmt"

// SimpleStore provides an interface for some very basic key/value
// storage needs.  Each SimpleStore (including ones created with Sub()
// should operate as seperate, flat key/value stores.
type SimpleStore interface {
	// Return a new SimpleStore that is subordinate to this one.
	// What exactly that means depends on the simplestore in question,
	// but it should wind up sharing the same backing store (directory, database, etcd cluster, whatever)
	Sub(string) (SimpleStore, error)
	// Return the list of keys that this store has in no particular order.
	Keys() ([]string, error)
	// Load the data for a particular key
	Load(string, interface{}) error
	// Save data to a key
	Save(string, interface{}) error
	// Remove a key/value pair.
	Remove(string) error
	// Encode and decode objects to be saved.
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
	// Control the writeability of the store
	ReadOnly() bool
	SetReadOnly() bool
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
