package storage

import (
	"bytes"
	"testing"
)

// These tests ARE your Phase 1 spec. Implement the TODOs in memtable.go and
// wal.go until every test here passes: `go test ./...`

func TestPutGet(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("name"), []byte("kestrel")); err != nil {
		t.Fatal(err)
	}
	v, found, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || !bytes.Equal(v, []byte("kestrel")) {
		t.Fatalf("got (%q, %v), want (kestrel, true)", v, found)
	}
}

func TestOverwrite(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put([]byte("k"), []byte("v1"))
	db.Put([]byte("k"), []byte("v2"))

	v, _, _ := db.Get([]byte("k"))
	if !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("got %q, want v2", v)
	}
}

func TestDelete(t *testing.T) {
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put([]byte("k"), []byte("v"))
	db.Delete([]byte("k"))

	if _, found, _ := db.Get([]byte("k")); found {
		t.Fatal("key should be gone after delete")
	}
}

// The headline test: data must survive a process restart, which proves the WAL
// works. We simulate a restart by closing the DB and reopening the same dir.
func TestDurabilityAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Delete([]byte("a"))
	db.Close() // simulate shutdown

	db2, err := Open(dir) // "restart"
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if _, found, _ := db2.Get([]byte("a")); found {
		t.Fatal("a was deleted before restart; it must stay gone")
	}
	v, found, _ := db2.Get([]byte("b"))
	if !found || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("b was lost across restart: got (%q, %v)", v, found)
	}
}
