package storage

import (
	"bytes"
	"testing"
)

// ---- Phase 1 tests (still must pass) ----

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

func TestDurabilityAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Delete([]byte("a"))
	db.Close()

	db2, err := Open(dir)
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

// ---- Phase 2 tests (SSTables) ----

// After a flush the memtable is empty, so these reads must be served from the
// on-disk SSTable.
func TestFlushThenGet(t *testing.T) {
	db, _ := Open(t.TempDir())
	defer db.Close()

	db.Put([]byte("k1"), []byte("v1"))
	db.Put([]byte("k2"), []byte("v2"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	if v, found, _ := db.Get([]byte("k1")); !found || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("k1 from sstable: (%q, %v)", v, found)
	}
	if v, found, _ := db.Get([]byte("k2")); !found || !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("k2 from sstable: (%q, %v)", v, found)
	}
}

// A newer SSTable must win over an older one for the same key.
func TestNewerValueShadowsOlderSSTable(t *testing.T) {
	db, _ := Open(t.TempDir())
	defer db.Close()

	db.Put([]byte("k"), []byte("old"))
	db.Flush() // sstable #1: k=old
	db.Put([]byte("k"), []byte("new"))
	db.Flush() // sstable #2: k=new

	if v, found, _ := db.Get([]byte("k")); !found || !bytes.Equal(v, []byte("new")) {
		t.Fatalf("expected newest value, got (%q, %v)", v, found)
	}
}

// The correctness trap: a tombstone must hide an older value in an older level,
// whether the tombstone lives in the memtable or in a newer SSTable.
func TestTombstoneShadowsSSTable(t *testing.T) {
	db, _ := Open(t.TempDir())
	defer db.Close()

	db.Put([]byte("k"), []byte("v"))
	db.Flush()             // sstable: k=v
	db.Delete([]byte("k")) // tombstone in memtable

	if _, found, _ := db.Get([]byte("k")); found {
		t.Fatal("tombstone in memtable must hide k=v in the sstable")
	}

	db.Flush() // tombstone now lives in a newer sstable
	if _, found, _ := db.Get([]byte("k")); found {
		t.Fatal("tombstone in sstable must still hide the older k=v")
	}
}

// Restart with a mix: some data flushed to SSTables, some only in the WAL.
func TestRestartWithSSTables(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir)
	db.Put([]byte("a"), []byte("1"))
	db.Flush()                       // a=1 -> sstable
	db.Put([]byte("b"), []byte("2")) // b=2 stays in memtable + WAL
	db.Close()

	db2, _ := Open(dir) // a from sstable, b from replayed WAL
	defer db2.Close()

	if v, found, _ := db2.Get([]byte("a")); !found || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("a after restart: (%q, %v)", v, found)
	}
	if v, found, _ := db2.Get([]byte("b")); !found || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("b after restart: (%q, %v)", v, found)
	}
}
