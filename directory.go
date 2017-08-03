package store

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type DirStore struct {
	*Codec
	Path string
}

func (f *DirStore) name(n string) string {
	return filepath.Join(f.Path, url.QueryEscape(n)) + f.Extension
}

func NewDirBackend(path string, codec *Codec) (*DirStore, error) {
	fullPath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, err
	}
	if codec == nil {
		codec = DefaultCodec
	}
	return &DirStore{
		Path:  path,
		Codec: codec,
	}, nil
}

func (f *DirStore) Sub(path string) (SimpleStore, error) {
	return NewDirBackend(filepath.Join(f.Path, path), f.Codec)
}

func (f *DirStore) Keys() ([]string, error) {
	d, err := os.Open(f.Path)
	if err != nil {
		return nil, err
	}
	names, err := d.Readdirnames(0)
	if err != nil {
		return nil, fmt.Errorf("dir keys: readdir error %#v", err)
	}
	res := make([]string, 0, len(names))
	for _, name := range names {
		if !strings.HasSuffix(name, f.Extension) {
			continue
		}
		n, err := url.QueryUnescape(strings.TrimSuffix(name, f.Extension))
		if err != nil {
			return nil, err
		}
		res = append(res, n)
	}
	return res[:], nil
}

func (f *DirStore) Load(key string) ([]byte, error) {
	return ioutil.ReadFile(f.name(key))
}

func (f *DirStore) List() ([][]byte, error) {
	return genericList(f)
}

func (f *DirStore) Save(key string, val []byte) error {
	file, err := os.Create(f.name(key))
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(val)
	if err != nil {
		os.Remove(file.Name())
		return err
	}
	file.Sync()
	return nil
}

func (f *DirStore) Remove(key string) error {
	return os.Remove(f.name(key))
}
