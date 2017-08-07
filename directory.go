package store

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Directory implements a Store that is backed by a local directory tree.
type Directory struct {
	storeBase
	Path string
}

func (f *Directory) name(n string) string {
	return filepath.Join(f.Path, url.QueryEscape(n)) + f.Ext()
}

func (f *Directory) Open(codec Codec) error {
	fullPath, err := filepath.Abs(filepath.Clean(f.Path))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return err
	}
	if codec == nil {
		codec = DefaultCodec
	}
	f.Codec = codec
	d, err := os.Open(fullPath)
	if err != nil {
		return err
	}
	infos, err := d.Readdir(0)
	if err != nil {
		return err
	}
	f.opened = true
	for _, info := range infos {
		if info.IsDir() && info.Name() != "." && info.Name() != ".." {
			if _, err := f.MakeSub(info.Name()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *Directory) MakeSub(path string) (Store, error) {
	f.Lock()
	defer f.Unlock()
	f.panicIfClosed()
	if child, ok := f.subStores[path]; ok {
		return child, nil
	}
	child := &Directory{Path: filepath.Join(f.Path, path)}
	err := child.Open(f.Codec)
	if err != nil {
		return nil, err
	}
	addSub(f, child, path)
	return child, nil
}

func (f *Directory) Keys() ([]string, error) {
	f.panicIfClosed()
	d, err := os.Open(f.Path)
	if err != nil {
		return nil, err
	}
	infos, err := d.Readdir(0)
	if err != nil {
		return nil, fmt.Errorf("dir keys: readdir error %#v", err)
	}
	res := []string{}
	for _, info := range infos {
		if info.IsDir() {
			continue
		}
		name := info.Name()
		if !strings.HasSuffix(name, f.Ext()) {
			continue
		}
		n, err := url.QueryUnescape(strings.TrimSuffix(name, f.Ext()))
		if err != nil {
			return nil, err
		}
		res = append(res, n)
	}
	return res, nil
}

func (f *Directory) Load(key string, val interface{}) error {
	f.panicIfClosed()
	buf, err := ioutil.ReadFile(f.name(key))
	if err != nil {
		return err
	}
	return f.Decode(buf, val)
}

func (f *Directory) Save(key string, val interface{}) error {
	f.panicIfClosed()
	if f.ReadOnly() {
		return UnWritable(key)
	}
	buf, err := f.Encode(val)
	if err != nil {
		return err
	}
	file, err := os.Create(f.name(key))
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(buf)
	if err != nil {
		os.Remove(file.Name())
		return err
	}
	file.Sync()
	return nil
}

func (f *Directory) Remove(key string) error {
	f.panicIfClosed()
	if f.ReadOnly() {
		return UnWritable(key)
	}
	return os.Remove(f.name(key))
}
