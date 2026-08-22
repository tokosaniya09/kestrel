package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// flushThresholdBytes is how large the memtable may grow before it is
// automatically flushed to an SSTable. Kept modest for real use; tests trigger
// flushes explicitly via Flush so they don't depend on this number.
const flushThresholdBytes = 4 << 20 // 4 MiB

// DB is the Phase 2 storage engine: an in-memory memtable in front of a stack of
// immutable on-disk SSTables, with a write-ahead log protecting the current
// memtable. It implements Engine.
//
// This file is written for you — it shows how the pieces compose. Your job is
// sstable.go; the flush and read paths here call into it.
type DB struct {
	mu       sync.Mutex
	dir      string
	wal      *WAL
	mem      *Memtable
	sstables []*SSTable // oldest first, newest last
	seq      int        // generation number for the next SSTable file
}

var _ Engine = (*DB)(nil)

// Open opens (creating if needed) a Kestrel storage engine rooted at dir. It
// loads any existing SSTables (oldest first) and replays the WAL into a fresh
// memtable, restoring the exact state from before shutdown or crash.
func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	db := &DB{dir: dir, mem: NewMemtable()}

	// Load existing SSTables. Filenames are zero-padded generation numbers, so
	// a lexical sort is also a chronological (oldest-first) sort.
	paths, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	for _, p := range paths {
		sst, err := OpenSSTable(p)
		if err != nil {
			return nil, err
		}
		db.sstables = append(db.sstables, sst)
	}
	if n := len(paths); n > 0 {
		var g int
		fmt.Sscanf(filepath.Base(paths[n-1]), "%06d.sst", &g)
		db.seq = g + 1
	}

	// Replay the WAL into the memtable (recovers writes not yet flushed).
	walPath := filepath.Join(dir, "wal.log")
	if _, err := os.Stat(walPath); err == nil {
		if err := Replay(walPath, func(r record) error {
			switch r.kind {
			case opPut:
				db.mem.Put(r.key, r.value)
			case opDelete:
				db.mem.Delete(r.key)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}
	db.wal = wal
	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.wal.Append(record{kind: opPut, key: key, value: value}); err != nil {
		return err
	}
	if err := db.wal.Sync(); err != nil {
		return err
	}
	db.mem.Put(key, value)
	return db.maybeFlush()
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.wal.Append(record{kind: opDelete, key: key}); err != nil {
		return err
	}
	if err := db.wal.Sync(); err != nil {
		return err
	}
	db.mem.Delete(key)
	return db.maybeFlush()
}

// Get consults the memtable first (freshest), then SSTables newest-to-oldest.
// The first source that CONTAINS the key wins — even if that entry is a
// tombstone, in which case the key is reported as deleted and older sources are
// not consulted. This is what makes deletes stick across levels.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if rec, ok := db.mem.Get(key); ok {
		if rec.kind == opDelete {
			return nil, false, nil
		}
		return rec.value, true, nil
	}
	for i := len(db.sstables) - 1; i >= 0; i-- {
		rec, ok, err := db.sstables[i].Get(key)
		if err != nil {
			return nil, false, err
		}
		if ok {
			if rec.kind == opDelete {
				return nil, false, nil
			}
			return rec.value, true, nil
		}
	}
	return nil, false, nil
}

func (db *DB) maybeFlush() error {
	if db.mem.Size() < flushThresholdBytes {
		return nil
	}
	return db.flushLocked()
}

// Flush forces the current memtable to disk as a new SSTable. A no-op if the
// memtable is empty. Exposed so tests and the REPL can flush deterministically.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.flushLocked()
}

// flushLocked writes the memtable to a new SSTable, then rotates the WAL. The
// ORDER matters for crash safety: the SSTable is made durable BEFORE the WAL is
// cleared, so a crash mid-flush can at worst replay already-flushed data —
// harmless, since the memtable shadows the SSTable with identical values.
func (db *DB) flushLocked() error {
	if db.mem.Size() == 0 {
		return nil
	}
	path := filepath.Join(db.dir, fmt.Sprintf("%06d.sst", db.seq))
	if err := writeSSTable(path, db.mem); err != nil {
		return err
	}
	sst, err := OpenSSTable(path)
	if err != nil {
		return err
	}
	db.sstables = append(db.sstables, sst)
	db.seq++
	db.mem = NewMemtable()

	// The WAL's contents are now redundant with the SSTable — rotate it.
	if err := db.wal.Close(); err != nil {
		return err
	}
	walPath := filepath.Join(db.dir, "wal.log")
	if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	wal, err := OpenWAL(walPath)
	if err != nil {
		return err
	}
	db.wal = wal
	return nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, s := range db.sstables {
		s.Close()
	}
	return db.wal.Close()
}
