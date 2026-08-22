package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

// This test exercises the SSTable in isolation — write a memtable out, read it
// back, and check the three lookup outcomes. Get this green before worrying
// about the DB-level Phase 2 tests; it pinpoints writer/reader bugs directly.
func TestSSTableRoundTrip(t *testing.T) {
	m := NewMemtable()
	m.Put([]byte("apple"), []byte("red"))
	m.Put([]byte("banana"), []byte("yellow"))
	m.Delete([]byte("cherry")) // a tombstone
	m.Put([]byte("date"), []byte("brown"))

	path := filepath.Join(t.TempDir(), "000001.sst")
	if err := writeSSTable(path, m); err != nil {
		t.Fatal(err)
	}
	sst, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	// 1. present value
	rec, ok, err := sst.Get([]byte("banana"))
	if err != nil || !ok || rec.kind != opPut || !bytes.Equal(rec.value, []byte("yellow")) {
		t.Fatalf("banana: got (%+v, %v, %v)", rec, ok, err)
	}
	// 2. present tombstone (ok == true, kind == opDelete)
	rec, ok, _ = sst.Get([]byte("cherry"))
	if !ok || rec.kind != opDelete {
		t.Fatalf("cherry should be present as a tombstone, got (%+v, %v)", rec, ok)
	}
	// 3. absent key
	if _, ok, _ := sst.Get([]byte("elderberry")); ok {
		t.Fatal("elderberry should not be present")
	}
}
