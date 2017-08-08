package store

import (
	"path"
	"strings"

	consul "github.com/hashicorp/consul/api"
)

// Consul implements a Store that is backed by the Consul key/value store.
type Consul struct {
	storeBase
	Client *consul.Client

	BaseKey string
}

func (c *Consul) Open(codec Codec) error {
	if codec == nil {
		codec = DefaultCodec
	}
	c.Codec = codec
	if c.Client == nil {
		client, err := consul.NewClient(consul.DefaultConfig())
		if err != nil {
			return err
		}
		if _, err = client.Agent().Self(); err != nil {
			return err
		}
		c.Client = client
	}
	keys, _, err := c.Client.KV().Keys(c.BaseKey, "", nil)
	if err != nil {
		return err
	}
	c.opened = true
	for i := range keys {
		if !strings.HasSuffix(keys[i], "/") {
			continue
		}
		subKey := strings.TrimSuffix(strings.TrimPrefix(keys[i], c.BaseKey+"/"), "/")
		if _, err := c.MakeSub(subKey); err != nil {
			return err
		}
	}
	c.closer = func() {
		c.Client = nil
	}
	return nil
}

func (b *Consul) MakeSub(prefix string) (Store, error) {
	b.Lock()
	defer b.Unlock()
	b.panicIfClosed()
	if res, ok := b.subStores[prefix]; ok {
		return res, nil
	}
	res := &Consul{Client: b.Client, BaseKey: b.BaseKey}
	err := res.Open(b.Codec)
	if err != nil {
		return nil, err
	}
	addSub(b, res, prefix)
	return res, nil
}

func (b *Consul) finalKey(k string) string {
	return path.Clean(path.Join(b.BaseKey, k))
}

func (b *Consul) Keys() ([]string, error) {
	b.panicIfClosed()
	keys, _, err := b.Client.KV().Keys(b.BaseKey, "", nil)
	if err != nil {
		return nil, err
	}
	res := []string{}
	for i := range keys {
		if strings.HasSuffix(keys[i], "/") {
			continue
		}
		res = append(res, strings.TrimPrefix(keys[i], b.BaseKey+"/"))
	}
	return res, nil
}

func (b *Consul) Load(key string, val interface{}) error {
	b.panicIfClosed()
	buf, _, err := b.Client.KV().Get(b.finalKey(key), nil)
	if buf == nil {
		return NotFound(key)
	}
	if err != nil {
		return err
	}
	return b.Decode(buf.Value, val)
}

func (b *Consul) Save(key string, val interface{}) error {
	b.panicIfClosed()
	if b.ReadOnly() {
		return UnWritable(key)
	}
	buf, err := b.Encode(val)
	if err != nil {
		return err
	}
	kp := &consul.KVPair{Value: buf, Key: b.finalKey(key)}
	_, err = b.Client.KV().Put(kp, nil)
	return err
}

func (b *Consul) Remove(key string) error {
	b.panicIfClosed()
	if b.ReadOnly() {
		return UnWritable(key)
	}
	_, err := b.Client.KV().Delete(b.finalKey(key), nil)
	return err
}
