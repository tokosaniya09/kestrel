package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// sstableIterator is a read cursor over one SSTable's records in ascending key
// order. It walks the in-memory index and reads each record from disk on demand.
type sstableIterator struct {
	sst *SSTable
	pos int
	rec record
	ok  bool
	err error
}

// Iterator returns a cursor positioned at the first record (if any).
func (s *SSTable) Iterator() *sstableIterator {
	it := &sstableIterator{sst: s, pos: -1}
	it.Next()
	return it
}

// Next advances to the following record. Afterwards Valid reports whether there
// is one, and Record returns it.
func (it *sstableIterator) Next() {
	it.pos++
	if it.pos >= len(it.sst.index) {
		it.ok = false
		return
	}
	off := it.sst.index[it.pos].offset
	if _, err := it.sst.f.Seek(off, io.SeekStart); err != nil {
		it.err, it.ok = err, false
		return
	}
	rec, err := decodeRecord(it.sst.f)
	if err != nil {
		it.err, it.ok = err, false
		return
	}
	it.rec, it.ok = rec, true
}

func (it *sstableIterator) Valid() bool    { return it.ok }
func (it *sstableIterator) Record() record { return it.rec }
func (it *sstableIterator) Err() error     { return it.err }

// Compact merges ALL current SSTables into a single new one (a "full
// compaction"), keeping only the newest version of each key and dropping
// tombstones, then removes the old files.
//
// It runs synchronously under the DB lock: no reads or writes happen during a
// compaction. (Background compaction is a later refinement.)
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(db.sstables) < 2 {
		return nil // nothing worth merging
	}

	// Build iterators, NEWEST FIRST. db.sstables is oldest-first, so walk it
	// backwards — the merge relies on index 0 being the newest source.
	iters := make([]*sstableIterator, 0, len(db.sstables))
	for i := len(db.sstables) - 1; i >= 0; i-- {
		iters = append(iters, db.sstables[i].Iterator())
	}

	merged, err := mergeSSTables(iters)
	if err != nil {
		return err
	}

	// Write the merged records as one new SSTable, reusing the memtable writer by
	// loading them into a fresh memtable first. Simplification: this holds the
	// compacted key set in memory; a production compactor streams to disk.
	m := NewMemtable()
	for _, r := range merged {
		m.Put(r.key, r.value) // merged output contains no tombstones
	}

	// Remember the old files so we can delete them after the swap. Capture paths
	// BEFORE closing (Windows won't remove an open file).
	old := db.sstables
	oldPaths := make([]string, len(old))
	for i, s := range old {
		oldPaths[i] = s.f.Name()
	}

	var newList []*SSTable
	if m.Size() > 0 {
		path := filepath.Join(db.dir, fmt.Sprintf("%06d.sst", db.seq))
		if err := writeSSTable(path, m); err != nil {
			return err
		}
		sst, err := OpenSSTable(path)
		if err != nil {
			return err
		}
		newList = []*SSTable{sst}
	}
	db.sstables = newList
	db.seq++

	// Release and delete the old files.
	//
	// Crash window: if we die between writing the new file and removing the old
	// ones, a restart loads BOTH. Reads stay correct (the new file has a higher
	// generation number, so it wins), but the old files leak until something
	// cleans them up. A manifest recording the live SSTable set would close this
	// window properly — see the limitations section in README.md.
	for _, s := range old {
		s.Close()
	}
	for _, p := range oldPaths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
