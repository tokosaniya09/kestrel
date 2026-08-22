// Package storage is Layer 1 of Kestrel: a durable, single-node key-value
// store built as an LSM-tree.
//
// In Phase 1 it is just a write-ahead log (WAL) plus an in-memory memtable.
// SSTables, Bloom filters and compaction arrive in later phases. Keep the
// public surface (the Engine interface) small and stable, because later the
// Raft state machine will drive an Engine by turning committed log entries
// into Put and Delete calls.
package storage

// Engine is the contract the rest of Kestrel depends on.
type Engine interface {
	// Get returns the value stored under key. found is false when the key is
	// absent, including when it was deleted.
	Get(key []byte) (value []byte, found bool, err error)

	// Put stores value under key, overwriting any previous value.
	Put(key, value []byte) error

	// Delete removes key. Reads afterwards report it as not found.
	Delete(key []byte) error

	// Close flushes and releases resources. The Engine is unusable afterwards.
	Close() error
}

// op identifies the kind of mutation recorded in the WAL (and later in
// SSTables). A delete is stored as a "tombstone": a real record whose presence
// means "this key is gone". An append-only store cannot erase in place, so it
// expresses deletion by writing a marker that shadows older values.
type op uint8

const (
	opPut    op = 0
	opDelete op = 1 // tombstone
)

// record is a single mutation. value is nil for a delete.
type record struct {
	kind  op
	key   []byte
	value []byte
}
