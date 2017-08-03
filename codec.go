package store

import (
	"encoding/json"

	"github.com/ghodss/yaml"
)

type codec struct {
	enc func(interface{}) ([]byte, error)
	dec func([]byte, interface{}) error
	ext string
}

func (c *codec) Encode(i interface{}) ([]byte, error) {
	return c.enc(i)
}

func (c *codec) Decode(buf []byte, i interface{}) error {
	return c.dec(buf, i)
}

func (c *codec) Ext() string {
	return c.ext
}

type Codec interface {
	Encode(interface{}) ([]byte, error)
	Decode([]byte, interface{}) error
	Ext() string
}

var JsonCodec = &codec{
	enc: json.Marshal,
	dec: json.Unmarshal,
	ext: ".json",
}

var YamlCodec = &codec{
	enc: yaml.Marshal,
	dec: yaml.Unmarshal,
	ext: ".yaml",
}

var DefaultCodec = JsonCodec
