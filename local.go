package store

import (
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
)

type SimpleLocalStore struct {
	Codec
	ro
	db     *bolt.DB
	bucket []byte
}

func (b *SimpleLocalStore) init(loc string) error {
	if b.db == nil {
		finalLoc := filepath.Clean(loc)
		if err := os.MkdirAll(finalLoc, 0755); err != nil {
			return err
		}
		db, err := bolt.Open(filepath.Join(finalLoc, "bolt.db"), 0600, nil)
		if err != nil {
			return err
		}
		b.db = db
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(b.bucket)
		return err
	})
}
func NewSimpleLocalStore(location string, codec Codec) (*SimpleLocalStore, error) {
	res := &SimpleLocalStore{Codec: codec, bucket: []byte(`Default`)}
	return res, res.init(location)
}

func (b *SimpleLocalStore) Sub(loc string) (SimpleStore, error) {
	res := &SimpleLocalStore{Codec: b.Codec, db: b.db, bucket: []byte(loc)}
	return res, res.init("")
}

func (b *SimpleLocalStore) Keys() ([]string, error) {
	res := []string{}
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		bucket.ForEach(func(k, v []byte) error {
			res = append(res, string(k))
			return nil
		})
		return nil
	})
	return res, err
}

func (b *SimpleLocalStore) List() ([][]byte, error) {
	res := [][]byte{}
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		return bucket.ForEach(func(k, v []byte) error {
			res = append(res, v)
			return nil
		})
	})
	return res, err
}

func (b *SimpleLocalStore) Load(key string) ([]byte, error) {
	var res []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		res = bucket.Get([]byte(key))
		if res == nil {
			return NotFound(key)
		}
		return nil
	})
	return res, err
}

func (b *SimpleLocalStore) Save(key string, val []byte) error {
	if b.ReadOnly() {
		return UnWritable(key)
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		return bucket.Put([]byte(key), val)
	})
}

func (b *SimpleLocalStore) Remove(key string) error {
	if b.ReadOnly() {
		return UnWritable(key)
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		if res := bucket.Get([]byte(key)); res == nil {
			return NotFound(key)
		}
		return bucket.Delete([]byte(key))
	})
}
