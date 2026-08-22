package storage

// Memtable is the in-memory, freshest layer of the LSM-tree. Reads consult it
// first. Phase 1 uses a simple map; swap in a skiplist before Phase 2 (SSTable
// flush needs sorted iteration).
//
// A delete is stored as a tombstone (record with kind == opDelete), NOT by
// removing the map entry — later the tombstone must shadow on-disk values.
type Memtable struct {
	data map[string]record
}

// NewMemtable returns an empty memtable.
func NewMemtable() *Memtable {
	return &Memtable{data: make(map[string]record)}
}

// Put inserts or overwrites key with value.
func (m *Memtable) Put(key, value []byte) {
	m.data[string(key)] = record{kind: opPut, key: key, value: value}
}

// Delete records a tombstone for key.
func (m *Memtable) Delete(key []byte) {
	m.data[string(key)] = record{kind: opDelete, key: key}
}

// Get returns (value, found). found is false if the key is absent OR present
// as a tombstone.
func (m *Memtable) Get(key []byte) (value []byte, found bool) {
	rec, ok := m.data[string(key)]
	if !ok || rec.kind == opDelete {
		return nil, false
	}
	return rec.value, true
}