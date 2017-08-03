package store

import (
	"encoding/json"

	"github.com/ghodss/yaml"
)

type Codec struct {
	enc       func(interface{}) ([]byte, error)
	dec       func([]byte, interface{}) error
	Extension string
}

func (c *Codec) Encode(i interface{}) ([]byte, error) {
	return c.enc(i)
}

func (c *Codec) Decode(buf []byte, i interface{}) error {
	return c.dec(buf, i)
}

var JsonCodec = &Codec{
	enc:       json.Marshal,
	dec:       json.Unmarshal,
	Extension: ".json",
}

var YamlCodec = &Codec{
	enc:       yaml.Marshal,
	dec:       yaml.Unmarshal,
	Extension: ".yaml",
}

var DefaultCodec = JsonCodec
