package store

import (
	"path"
	"strings"

	consul "github.com/hashicorp/consul/api"
)

type SimpleConsulStore struct {
	storeBase
	client *consul.Client

	baseKey string
}

func NewSimpleConsulStore(c *consul.Client, prefix string, codec Codec) (*SimpleConsulStore, error) {
	if codec == nil {
		codec = DefaultCodec
	}
	res := &SimpleConsulStore{client: c, baseKey: prefix}
	res.Codec = codec
	keys, _, err := res.client.KV().Keys(res.baseKey, "", nil)
	if err != nil {
		return nil, err
	}
	for i := range keys {
		if !strings.HasSuffix(keys[i], "/") {
			continue
		}
		subKey := strings.TrimSuffix(strings.TrimPrefix(keys[i], res.baseKey+"/"), "/")
		if _, err := res.MakeSub(subKey); err != nil {
			return nil, err
		}
	}
	res.closer = func() {
		res.client = nil
	}
	return res, nil
}

func (b *SimpleConsulStore) MakeSub(prefix string) (SimpleStore, error) {
	b.Lock()
	defer b.Unlock()
	b.panicIfClosed()
	if res, ok := b.subStores[prefix]; ok {
		return res, nil
	}
	res, err := NewSimpleConsulStore(b.client, path.Join(b.baseKey, prefix), b.Codec)
	if err != nil {
		return nil, err
	}
	addSub(b, res, prefix)
	return res, nil
}

func (b *SimpleConsulStore) finalKey(k string) string {
	return path.Clean(path.Join(b.baseKey, k))
}

func (b *SimpleConsulStore) Keys() ([]string, error) {
	b.panicIfClosed()
	keys, _, err := b.client.KV().Keys(b.baseKey, "", nil)
	if err != nil {
		return nil, err
	}
	res := []string{}
	for i := range keys {
		if strings.HasSuffix(keys[i], "/") {
			continue
		}
		res = append(res, strings.TrimPrefix(keys[i], b.baseKey+"/"))
	}
	return res, nil
}

func (b *SimpleConsulStore) Load(key string, val interface{}) error {
	b.panicIfClosed()
	buf, _, err := b.client.KV().Get(b.finalKey(key), nil)
	if buf == nil {
		return NotFound(key)
	}
	if err != nil {
		return err
	}
	return b.Decode(buf.Value, val)
}

func (b *SimpleConsulStore) Save(key string, val interface{}) error {
	b.panicIfClosed()
	if b.ReadOnly() {
		return UnWritable(key)
	}
	buf, err := b.Encode(val)
	if err != nil {
		return err
	}
	kp := &consul.KVPair{Value: buf, Key: b.finalKey(key)}
	_, err = b.client.KV().Put(kp, nil)
	return err
}

func (b *SimpleConsulStore) Remove(key string) error {
	b.panicIfClosed()
	if b.ReadOnly() {
		return UnWritable(key)
	}
	_, err := b.client.KV().Delete(b.finalKey(key), nil)
	return err
}
