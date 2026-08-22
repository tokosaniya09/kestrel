package storage

// Memtable is the in-memory, freshest layer of the LSM-tree. Reads consult it
// first. When it grows past a size threshold it is frozen and flushed to an
// SSTable (Phase 2) — and that flush must walk the keys in sorted order, so
// plan to upgrade the internals to a skiplist before Phase 2.
//
// Phase 1 recommendation: start with the dead-simple map-backed version below
// so you can get the whole WAL + memtable + restart loop working and actually
// SEE it run. Swap in a skiplist as a focused task later.
//
// IMPORTANT: a delete is stored as a tombstone (a record with kind == opDelete),
// NOT by removing the map entry. Later, that tombstone has to shadow older
// values that live in SSTables; if you just delete the map entry now, you'll
// build the wrong habit for Phase 2+.
type Memtable struct {
	// TODO(you): choose a backing store. Simplest: data map[string]record
}

// NewMemtable returns an empty memtable.
func NewMemtable() *Memtable {
	// TODO(you): initialise and return the struct (make the map)
	panic("TODO: implement NewMemtable")
}

// Put inserts or overwrites key with value.
func (m *Memtable) Put(key, value []byte) {
	// TODO(you): store record{kind: opPut, key: key, value: value}
	panic("TODO: implement Memtable.Put")
}

// Delete records a tombstone for key.
func (m *Memtable) Delete(key []byte) {
	// TODO(you): store record{kind: opDelete, key: key}
	panic("TODO: implement Memtable.Delete")
}

// Get returns (value, found). found is false if the key is absent OR present
// as a tombstone.
func (m *Memtable) Get(key []byte) (value []byte, found bool) {
	// TODO(you): look up the record.
	//   - missing            -> (nil, false)
	//   - kind == opDelete    -> (nil, false)   // tombstone shadows the value
	//   - otherwise           -> (rec.value, true)
	panic("TODO: implement Memtable.Get")
}
