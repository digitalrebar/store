package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
)

type File struct {
	storeBase
	Path string
	vals map[string][]byte
}

func (f *File) MakeSub(path string) (Store, error) {
	f.Lock()
	defer f.Unlock()
	f.panicIfClosed()
	if child, ok := f.subStores[path]; ok {
		return child, nil
	}
	sub := &File{}
	sub.Codec = f.Codec
	sub.vals = map[string][]byte{}
	sub.opened = true
	addSub(f, sub, path)
	return sub, nil
}

func (f *File) mux() *sync.RWMutex {
	f.RLock()
	defer f.RUnlock()
	if f.parentStore != nil {
		return f.parentStore.(*File).mux()
	}
	return &f.RWMutex
}

func (f *File) open(vals map[string]interface{}) error {
	f.vals = map[string][]byte{}
	for k, v := range vals {
		if k == "sections" {
			subSections, ok := v.(map[string]interface{})
			if !ok {
				return fmt.Errorf("Invalid sections declaration: %#v", v)
			}
			for subName, subVals := range subSections {
				sub := &File{}
				sub.Codec = f.Codec
				if err := sub.open(subVals.(map[string]interface{})); err != nil {
					return err
				}
				addSub(f, sub, subName)
			}
		} else {
			buf, err := f.Encode(v)
			if err != nil {
				return err
			}
			f.vals[k] = buf
		}
	}
	f.opened = true
	return nil
}

func (f *File) Open(codec Codec) error {
	fullPath, err := filepath.Abs(filepath.Clean(f.Path))
	if err != nil {
		return err
	}
	if codec == nil {
		codec = DefaultCodec
	}
	f.Codec = codec
	vals := map[string]interface{}{}
	if err := os.MkdirAll(path.Dir(fullPath), 0755); err != nil {
		return err
	}

	buf, err := ioutil.ReadFile(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if buf != nil {
		if err := f.Decode(buf, &vals); err != nil {
			return err
		}
	}
	return f.open(vals)
}

func (f *File) Keys() ([]string, error) {
	mux := f.mux()
	mux.RLock()
	defer mux.RUnlock()
	f.panicIfClosed()
	res := make([]string, 0, len(f.vals))
	for k := range f.vals {
		res = append(res, k)
	}
	return res, nil
}

func (f *File) Load(key string, val interface{}) error {
	mux := f.mux()
	mux.RLock()
	defer mux.RUnlock()
	f.panicIfClosed()
	buf, ok := f.vals[key]
	if ok {
		return f.Decode(buf, val)
	}
	return NotFound(key)
}

func (f *File) prepSave() (map[string]interface{}, error) {
	res := map[string]interface{}{}
	for k, v := range f.vals {
		var obj interface{}
		if err := f.Decode(v, &obj); err != nil {
			return nil, err
		}
		res[k] = obj
	}
	if len(f.subStores) == 0 {
		return res, nil
	}
	subs := map[string]interface{}{}
	for subName, subStore := range f.subStores {
		subVals, err := subStore.(*File).prepSave()
		if err != nil {
			return nil, err
		}
		subs[subName] = subVals
	}
	res["sections"] = subs
	return res, nil
}

func (f *File) save() error {
	f.panicIfClosed()
	if f.parentStore != nil {
		parent := f.parentStore.(*File)
		return parent.save()
	}
	toSave, err := f.prepSave()
	if err != nil {
		return err
	}
	buf, err := f.Encode(toSave)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(f.Path, buf, 0644)
}

func (f *File) Save(key string, val interface{}) error {
	mux := f.mux()
	mux.Lock()
	defer mux.Unlock()
	if f.readOnly {
		return UnWritable(key)
	}
	buf, err := f.Encode(val)
	if err != nil {
		return err
	}
	f.vals[key] = buf
	return f.save()
}

func (f *File) Remove(key string) error {
	mux := f.mux()
	mux.Lock()
	defer mux.Unlock()
	if f.readOnly {
		return UnWritable(key)
	}
	if _, ok := f.vals[key]; !ok {
		return NotFound(key)
	}
	delete(f.vals, key)
	return f.save()
}
