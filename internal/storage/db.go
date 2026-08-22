package storage

import (
	"os"
	"path/filepath"
)

// DB is the Phase 1 storage engine: a WAL plus a memtable. It implements Engine.

type DB struct {
	dir string
	wal *WAL
	mem *Memtable
}

// Compile-time assertion that *DB satisfies the Engine interface. If you break
// the interface, this line fails to build — an early warning.
var _ Engine = (*DB)(nil)

// Open opens (creating if needed) a Kestrel storage engine rooted at dir.
// It first replays any existing WAL to rebuild the memtable, which is exactly
// what gives us crash recovery: restarting the process restores prior state.
func Open(dir string) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	walPath := filepath.Join(dir, "wal.log")

	mem := NewMemtable()

	// Recovery: if a WAL already exists, replay it into the fresh memtable
	// BEFORE we reopen the WAL for appending.
	if _, err := os.Stat(walPath); err == nil {
		if err := Replay(walPath, func(r record) error {
			switch r.kind {
			case opPut:
				mem.Put(r.key, r.value)
			case opDelete:
				mem.Delete(r.key)
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
	return &DB{dir: dir, wal: wal, mem: mem}, nil
}

func (db *DB) Put(key, value []byte) error {
	// Write-ahead rule: log first and make it durable, THEN touch memory.
	if err := db.wal.Append(record{kind: opPut, key: key, value: value}); err != nil {
		return err
	}
	if err := db.wal.Sync(); err != nil {
		return err
	}
	db.mem.Put(key, value)
	return nil
}

func (db *DB) Delete(key []byte) error {
	if err := db.wal.Append(record{kind: opDelete, key: key}); err != nil {
		return err
	}
	if err := db.wal.Sync(); err != nil {
		return err
	}
	db.mem.Delete(key)
	return nil
}

func (db *DB) Get(key []byte) ([]byte, bool, error) {
	// Phase 1: everything lives in the memtable. In Phase 2 this method also
	// consults the SSTables (newest first) when the memtable misses.
	v, found := db.mem.Get(key)
	return v, found, nil
}

func (db *DB) Close() error {
	return db.wal.Close()
}
