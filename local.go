package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/boltdb/bolt"
)

type SimpleLocalStore struct {
	storeBase
	db     *bolt.DB
	bucket []byte
}

func (b *SimpleLocalStore) getBucket(tx *bolt.Tx) (res *bolt.Bucket) {
	for _, part := range bytes.Split(b.bucket, []byte("/")) {
		if res == nil {
			res = tx.Bucket(part)
		} else {
			res = res.Bucket(part)
		}
		if res == nil {
			panic(fmt.Sprintf("Bucket %s does not exist", string(b.bucket)))
		}
	}
	return
}

func (l *SimpleLocalStore) loadSubs() error {
	err := l.db.Update(func(tx *bolt.Tx) error {
		var bukkit *bolt.Bucket
		var err error
		for _, part := range bytes.Split(l.bucket, []byte("/")) {
			if bukkit == nil {
				bukkit, err = tx.CreateBucketIfNotExists(part)
			} else {
				bukkit, err = bukkit.CreateBucketIfNotExists(part)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	subs := [][]byte{}
	err = l.db.View(func(tx *bolt.Tx) error {
		bucket := l.getBucket(tx)
		bucket.ForEach(func(k, v []byte) error {
			if v == nil {
				subs = append(subs, k)
			}
			return nil
		})
		return nil
	})
	if err != nil {
		return err
	}

	for _, sub := range subs {
		if _, err := l.MakeSub(string(sub)); err != nil {
			return err
		}
	}
	return nil
}

func NewSimpleLocalStore(location string, codec Codec) (*SimpleLocalStore, error) {
	res := &SimpleLocalStore{bucket: []byte(`Default`)}
	res.Codec = codec
	finalLoc := filepath.Clean(location)
	if err := os.MkdirAll(finalLoc, 0755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(finalLoc, "bolt.db"), 0600, nil)
	if err != nil {
		return nil, err
	}
	res.db = db
	err = res.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(res.bucket)
		return err
	})
	if err != nil {
		return nil, err
	}

	if err := res.loadSubs(); err != nil {
		return nil, err
	}

	res.closer = func() {
		res.db.Close()
		res.db = nil
	}
	return res, nil
}

func (b *SimpleLocalStore) MakeSub(loc string) (SimpleStore, error) {
	b.Lock()
	defer b.Unlock()
	b.panicIfClosed()
	if res, ok := b.subStores[loc]; ok {
		return res, nil
	}
	res := &SimpleLocalStore{db: b.db, bucket: bytes.Join([][]byte{b.bucket, []byte(loc)}, []byte("/"))}
	res.Codec = b.Codec
	if err := res.loadSubs(); err != nil {
		return nil, err
	}

	res.closer = func() {
		res.db = nil
	}
	addSub(b, res, loc)
	return res, nil
}

func (b *SimpleLocalStore) Keys() ([]string, error) {
	b.panicIfClosed()
	res := []string{}
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := b.getBucket(tx)
		bucket.ForEach(func(k, v []byte) error {
			if v != nil {
				res = append(res, string(k))
			}
			return nil
		})
		return nil
	})
	return res, err
}

func (b *SimpleLocalStore) Load(key string, val interface{}) error {
	b.panicIfClosed()
	var res []byte
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := b.getBucket(tx)
		res = bucket.Get([]byte(key))
		if res == nil {
			return NotFound(key)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return b.Decode(res, val)
}

func (b *SimpleLocalStore) Save(key string, val interface{}) error {
	b.panicIfClosed()
	if b.ReadOnly() {
		return UnWritable(key)
	}
	buf, err := b.Encode(val)
	if err != nil {
		return err
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.getBucket(tx)
		return bucket.Put([]byte(key), buf)
	})
}

func (b *SimpleLocalStore) Remove(key string) error {
	b.panicIfClosed()
	if b.ReadOnly() {
		return UnWritable(key)
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := b.getBucket(tx)
		if res := bucket.Get([]byte(key)); res == nil {
			return NotFound(key)
		}
		return bucket.Delete([]byte(key))
	})
}
