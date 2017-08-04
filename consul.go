package store

import (
	"path"
	"strings"

	consul "github.com/hashicorp/consul/api"
)

type SimpleConsulStore struct {
	storeBase
	kv      *consul.KV
	baseKey string
}

func NewSimpleConsulStore(c *consul.Client, prefix string, codec Codec) (*SimpleConsulStore, error) {
	if codec == nil {
		codec = DefaultCodec
	}
	res := &SimpleConsulStore{kv: c.KV(), baseKey: prefix}
	res.Codec = codec
	return res, nil
}

func (b *SimpleConsulStore) MakeSub(prefix string) (SimpleStore, error) {
	res := &SimpleConsulStore{kv: b.kv, baseKey: path.Join(b.baseKey, prefix)}
	res.Codec = b.Codec
	addSub(b, res, prefix)
	return res, nil
}

func (b *SimpleConsulStore) finalKey(k string) string {
	return path.Clean(path.Join(b.baseKey, k))
}

func (b *SimpleConsulStore) Keys() ([]string, error) {
	keys, _, err := b.kv.Keys(b.baseKey, "", nil)
	if err != nil {
		return nil, err
	}
	res := make([]string, len(keys))
	for i := range keys {
		res[i] = strings.TrimPrefix(keys[i], b.baseKey+"/")
	}
	return res, nil
}

func (b *SimpleConsulStore) Load(key string, val interface{}) error {
	buf, _, err := b.kv.Get(b.finalKey(key), nil)
	if buf == nil {
		return NotFound(key)
	}
	if err != nil {
		return err
	}
	return b.Decode(buf.Value, val)
}

func (b *SimpleConsulStore) Save(key string, val interface{}) error {
	buf, err := b.Encode(val)
	if err != nil {
		return err
	}
	kp := &consul.KVPair{Value: buf, Key: b.finalKey(key)}
	_, err = b.kv.Put(kp, nil)
	return err
}

func (b *SimpleConsulStore) Remove(key string) error {
	_, err := b.kv.Delete(b.finalKey(key), nil)
	return err
}
