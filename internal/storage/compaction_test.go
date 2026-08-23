package storage

import (
	"bytes"
	"path/filepath"
	"testing"
)

func sstCount(t *testing.T, db *DB) int {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(db.dir, "*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	return len(paths)
}

// Three flushes -> three SSTables -> one after compaction, newest values kept.
func TestCompactionMergesToOneFile(t *testing.T) {
	db, _ := Open(t.TempDir())
	defer db.Close()

	db.Put([]byte("k"), []byte("1"))
	db.Flush()
	db.Put([]byte("k"), []byte("2"))
	db.Flush()
	db.Put([]byte("j"), []byte("9"))
	db.Flush()

	if got := sstCount(t, db); got != 3 {
		t.Fatalf("expected 3 sstables before compaction, got %d", got)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := sstCount(t, db); got != 1 {
		t.Fatalf("expected 1 sstable after compaction, got %d", got)
	}

	if v, ok, _ := db.Get([]byte("k")); !ok || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("k after compaction: (%q, %v), want 2", v, ok)
	}
	if v, ok, _ := db.Get([]byte("j")); !ok || !bytes.Equal(v, []byte("9")) {
		t.Fatalf("j after compaction: (%q, %v), want 9", v, ok)
	}
}

// A tombstone that is the newest version of its key must remove the key AND be
// dropped from the merged file (full-compaction rule).
func TestCompactionDropsTombstones(t *testing.T) {
	db, _ := Open(t.TempDir())
	defer db.Close()

	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("1"))
	db.Flush() // sst1: a=1, b=1
	db.Delete([]byte("a"))
	db.Flush() // sst2: tombstone a

	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := sstCount(t, db); got != 1 {
		t.Fatalf("expected 1 sstable, got %d", got)
	}
	if _, ok, _ := db.Get([]byte("a")); ok {
		t.Fatal("a was deleted; must stay gone after compaction")
	}
	if v, ok, _ := db.Get([]byte("b")); !ok || !bytes.Equal(v, []byte("1")) {
		t.Fatalf("b after compaction: (%q, %v), want 1", v, ok)
	}
}

// Compacted state must survive a restart, and still be a single file.
func TestCompactionThenRestart(t *testing.T) {
	dir := t.TempDir()

	db, _ := Open(dir)
	db.Put([]byte("x"), []byte("1"))
	db.Flush()
	db.Put([]byte("x"), []byte("2"))
	db.Put([]byte("y"), []byte("7"))
	db.Flush()
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, _ := Open(dir)
	defer db2.Close()
	if v, ok, _ := db2.Get([]byte("x")); !ok || !bytes.Equal(v, []byte("2")) {
		t.Fatalf("x after restart: (%q, %v), want 2", v, ok)
	}
	if v, ok, _ := db2.Get([]byte("y")); !ok || !bytes.Equal(v, []byte("7")) {
		t.Fatalf("y after restart: (%q, %v), want 7", v, ok)
	}
	if got := sstCount(t, db2); got != 1 {
		t.Fatalf("expected 1 sstable after restart, got %d", got)
	}
}

// With fewer than two SSTables there's nothing to merge; Compact is a no-op.
func TestCompactionNoOpWithFewSSTables(t *testing.T) {
	db, _ := Open(t.TempDir())
	defer db.Close()

	db.Put([]byte("k"), []byte("v"))
	db.Flush() // 1 sstable
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if got := sstCount(t, db); got != 1 {
		t.Fatalf("compaction with <2 sstables should be a no-op, got %d files", got)
	}
	if v, ok, _ := db.Get([]byte("k")); !ok || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("k should be unchanged: (%q, %v)", v, ok)
	}
}
