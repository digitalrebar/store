package store

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

var (
	currentStore Store
	failed       = errors.New("Failed hook")
)

type TestVal struct {
	Name     string
	Val      string
	hook     string
	failHook bool
}

func (t *TestVal) doHook(hook string) error {
	t.hook = hook
	if t.failHook {
		return failed
	}
	return nil
}

func (t *TestVal) Prefix() string {
	return "testVal"
}

func (t *TestVal) Key() string {
	return t.Name
}

func (t *TestVal) New() KeySaver {
	return &TestVal{}
}

func (t *TestVal) Backend() Store {
	return currentStore
}

func (t *TestVal) OnLoad() error {
	return t.doHook("OnLoad")
}

func (t *TestVal) OnChange(KeySaver) error {
	return t.doHook("OnChange")
}

func (t *TestVal) BeforeDelete() error {
	return t.doHook("BeforeDelete")
}

func (t *TestVal) AfterDelete() {
	t.doHook("AfterDelete")
}

func (t *TestVal) OnCreate() error {
	return t.doHook("OnCreate")
}

func (t *TestVal) BeforeSave() error {
	return t.doHook("BeforeSave")
}

func (t *TestVal) AfterSave() {
	t.doHook("AfterSave")
}

type op func(KeySaver) (bool, error)
type test struct {
	op       op
	name     string
	val      *TestVal
	pass     bool
	hookFail bool
	lastHook string
}

var tv = TestVal{"Name", "Value", "", false}
var ntv = TestVal{"Uncreated", "Value", "", false}
var tests = []test{
	test{op(Create), "Create Hook Fail", &tv, false, true, "OnCreate"},
	test{op(Create), "Create Succeed", &tv, true, false, "AfterSave"},
	test{op(Create), "Create Duplicate Fail", &tv, false, false, ""},
	test{op(Load), "Load Hook Fail", &tv, true, true, "OnLoad"},
	test{op(Load), "Load Nonexistent Fail", &ntv, false, false, ""},
	test{op(Load), "Load Succeed", &tv, true, false, "OnLoad"},
	test{op(Save), "Save Before Hook Fail", &tv, false, true, "BeforeSave"},
	test{op(Save), "Save Succeed", &tv, true, false, "AfterSave"},
	test{op(Update), "Update Before Hook Fail", &tv, false, true, "OnChange"},
	test{op(Update), "Update Succeed", &tv, true, false, "AfterSave"},
	test{op(Remove), "Remove Hook Fail", &tv, false, true, "BeforeDelete"},
	test{op(Remove), "Remove Success", &tv, true, false, "AfterDelete"},
	test{op(Remove), "Remove Nonexistent Fail", &ntv, false, false, "BeforeDelete"},
}

var createTests = []test{
	test{op(Create), "Create Hook Fail", &tv, false, true, "OnCreate"},
	test{op(Create), "Create Succeed", &tv, true, false, "AfterSave"},
	test{op(Create), "Create Duplicate Fail", &tv, false, false, ""},
}

var roTests = []test{
	test{op(Load), "Load Hook Fail", &tv, true, true, "OnLoad"},
	test{op(Load), "Load Nonexistent Fail", &ntv, false, false, ""},
	test{op(Load), "Load Succeed", &tv, true, false, "OnLoad"},
	test{op(Save), "Save Before Hook Fail", &tv, false, true, "BeforeSave"},
	test{op(Save), "Save Succeed", &tv, false, false, "BeforeSave"},
	test{op(Update), "Update Before Hook Fail", &tv, false, true, "OnChange"},
	test{op(Update), "Update Succeed", &tv, false, false, "BeforeSave"},
	test{op(Remove), "Remove Hook Fail", &tv, false, true, "BeforeDelete"},
	test{op(Remove), "Remove Success", &tv, false, false, "BeforeDelete"},
	test{op(Remove), "Remove Nonexistent Fail", &ntv, false, false, "BeforeDelete"},
}

func runTests(t *testing.T, toRun []test) {
	for _, s := range toRun {
		expectedTo := "fail"
		if s.pass {
			expectedTo = "pass"
		}
		actuallyDid := "fail"
		s.val.hook = ""
		s.val.failHook = s.hookFail
		ok, err := s.op(s.val)
		if ok {
			actuallyDid = "pass"
		}
		passMsg := fmt.Sprintf("%s: Expected to %s, actually %s", s.name, expectedTo, actuallyDid)
		hookMsg := fmt.Sprintf("%s: Expected last hook to be `%s`, was `%s`", s.name, s.lastHook, s.val.hook)
		if ok != s.pass {
			t.Error(passMsg)
		} else {
			t.Log(passMsg)
		}
		if s.lastHook != s.val.hook {
			t.Error(hookMsg)
		} else {
			t.Log(hookMsg)
		}
		if s.hookFail {
			if err == nil {
				t.Errorf("%s: Expected hook to fail, but got no error!", s.name)
			} else if err != failed {
				t.Errorf("%s: Expected hook to fail with `%v`, but got `%v`", s.name, failed, err)
			} else {
				t.Logf("%s: Got expected hook failure `%v`", s.name, failed)
			}
		} else if !s.pass {
			if err != nil {
				t.Logf("%s: Got error %v", s.name, err)
			} else {
				t.Logf("%s: Expected to fail, but got no error!", s.name)
			}
		}
	}
}

// Expects a freshly-created store
func testOneStore(t *testing.T, s Store) {
	currentStore = s
	runTests(t, tests)
	// At the end, the store should be empty
	ents, err := currentStore.Keys()
	if err != nil {
		t.Errorf("Error listing store: %v", err)
	} else if len(ents) != 0 {
		t.Errorf("Too many entries in store: wanted 0, got %d", len(ents))
	}
	ok, err := Create(&tv)
	if !ok {
		t.Errorf("Failed to create an entry for the list test: %v", err)
	}
	nents, err := List(&tv)
	if err != nil {
		t.Errorf("Error listing the entries for the list test: %v", err)
	} else if len(nents) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(nents))
	}
	keys, err := currentStore.Keys()
	if err != nil {
		t.Errorf("Error getting keys for the keys test: %v", err)
	} else if len(keys) != 1 {
		t.Errorf("Expected 1 key, got %d", len(keys))
	}
	for _, k := range keys {
		if err := currentStore.Remove(k); err != nil {
			t.Errorf("Error removing key %s", k)
		}
	}
	runTests(t, createTests)
	if !currentStore.SetReadOnly() {
		t.Errorf("Unable to set store to read only")
	}
	runTests(t, roTests)
}

func testStore(t *testing.T, s Store) {
	t.Logf("Testing top-level store")
	testOneStore(t, s)
	var err error
	_, err = s.MakeSub("sub1")
	if err != nil {
		t.Errorf("Error creating substore sub1")
		return
	}
	t.Logf("Testing substore sub1")
	sub1 := s.GetSub("sub1")
	testOneStore(t, sub1)
	_, err = sub1.MakeSub("sub2")
	if err != nil {
		t.Errorf("Error creating substore sub2")
		return
	}
	t.Logf("Testing substore sub2")
	testOneStore(t, sub1.GetSub("sub2"))
}

func TestMemoryStore(t *testing.T) {
	s := &Memory{}
	s.Open(nil)
	t.Log("Testing simple memory store")
	testStore(t, s)
	t.Log("Memory store test finished")
}

func testPersistent(t *testing.T, storeType, storeCodec string) {
	t.Logf("Creating tmpdir for persistent testing")
	tmpDir, err := ioutil.TempDir("", "store-")
	if err != nil {
		t.Errorf("Failed to create tmp dir for persistent testing")
		return
	}
	t.Logf("Running in %s", tmpDir)
	defer os.RemoveAll(tmpDir)
	var storeLoc string
	if storeType == "file" {
		storeLoc = path.Join(tmpDir, "data."+storeCodec)
	} else {
		storeLoc = tmpDir
	}
	storeURI := fmt.Sprintf("%s://%s?codec=%s", storeType, storeLoc, storeCodec)
	s, err := Open(storeURI)
	if err != nil {
		t.Errorf("Failed to create store: %v", err)
		return
	}
	t.Logf("Store: %#v", s)
	t.Log("Testing store")
	testStore(t, s)
	t.Log("Testing persistence of local substore hierarchy")
	s.Close()
	s, err = Open(storeURI)
	if err != nil {
		t.Errorf("Failed to reload store: %v", err)
		return
	}
	sub1 := s.GetSub("sub1")
	if sub1 == nil {
		t.Errorf("Did not load expected substore sub1")
		return
	}
	sub2 := sub1.GetSub("sub2")
	if sub2 == nil {
		t.Errorf("Did not load expected substore sub2")
		return
	}
	t.Logf("Persistent test finished")
}

func TestPersistentStores(t *testing.T) {
	storeCodecs := []string{"json", "yaml", "default"}
	storeType := []string{"bolt", "directory", "file"}
	for _, codec := range storeCodecs {
		for _, storeType := range storeType {
			t.Logf("Testing persistent store %s with codec %s", storeType, codec)
			testPersistent(t, storeType, codec)
			t.Logf("--------------------------------------------------------")
		}
	}
}
