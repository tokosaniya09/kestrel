package storage

import (
	"bytes"
	"math/rand"
)

// Memtable is the in-memory, freshest layer of the LSM-tree, kept sorted by key
// so it can be flushed to a sorted SSTable and (later) range-scanned.
//
// It is implemented as a SKIPLIST: an ordered linked list with a few "express
// lane" levels on top that let a search skip far ahead, giving O(log n) average
// insert and lookup without the rebalancing bookkeeping of a tree. Each node's
// height is decided by repeated coin flips, so higher levels are exponentially
// sparser. Picture it like this (x = a node present at that level):
//
//	L2:  head --------------------> x --------------------> nil
//	L1:  head ------> x ----------> x ----------> x ------> nil
//	L0:  head -> x -> x -> x -> x -> x -> x -> x -> x ----> nil   (every node)
//
// A delete is stored as a TOMBSTONE (a record with kind == opDelete), never by
// removing the node — the tombstone has to shadow older values living in
// SSTables, so it must remain visible until compaction removes it (Phase 3).
type Memtable struct {
	head  *slNode
	level int // number of levels currently in use (>= 1)
	size  int // approximate bytes held, used to decide when to flush
	rng   *rand.Rand
}

const (
	slMaxLevel = 16  // plenty for millions of entries
	slP        = 0.5 // probability a node is promoted to the next level up
)

type slNode struct {
	rec  record
	next []*slNode // next[i] is the successor node at level i
}

// NewMemtable returns an empty memtable.
func NewMemtable() *Memtable {
	return &Memtable{
		head:  &slNode{next: make([]*slNode, slMaxLevel)},
		level: 1,
		// Fixed seed => deterministic structure => reproducible tests. A real
		// system would seed from time/crypto rand; determinism helps you debug.
		rng: rand.New(rand.NewSource(1)),
	}
}

// Size reports the approximate number of bytes held, for the flush threshold.
func (m *Memtable) Size() int { return m.size }

// randomLevel returns a height in [1, slMaxLevel], each extra level with
// probability slP. This is what keeps the skiplist balanced on average.
func (m *Memtable) randomLevel() int {
	lvl := 1
	for lvl < slMaxLevel && m.rng.Float64() < slP {
		lvl++
	}
	return lvl
}

// put inserts a new record or overwrites the existing one for its key.
func (m *Memtable) put(r record) {
	// update[i] will hold the last node at level i whose key < r.key — i.e. the
	// node whose next-pointer we must splice through when inserting.
	update := make([]*slNode, slMaxLevel)
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].rec.key, r.key) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}

	// The node just past update[0] is the first with key >= r.key.
	if n := x.next[0]; n != nil && bytes.Equal(n.rec.key, r.key) {
		// Key already exists: overwrite in place, adjust the size estimate.
		m.size += len(r.value) - len(n.rec.value)
		n.rec = r
		return
	}

	lvl := m.randomLevel()
	if lvl > m.level {
		// New node is taller than anything so far: the head links those new
		// upper levels straight to it.
		for i := m.level; i < lvl; i++ {
			update[i] = m.head
		}
		m.level = lvl
	}

	n := &slNode{rec: r, next: make([]*slNode, lvl)}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	m.size += len(r.key) + len(r.value) + 8 // + rough per-entry overhead
}

// Put inserts or overwrites key with value.
func (m *Memtable) Put(key, value []byte) {
	m.put(record{kind: opPut, key: key, value: value})
}

// Delete records a tombstone for key.
func (m *Memtable) Delete(key []byte) {
	m.put(record{kind: opDelete, key: key})
}

// Get returns the stored record for key and whether it is PRESENT in this
// memtable. "Present" includes a tombstone (kind == opDelete): that is what lets
// a caller tell "deleted here — stop looking" apart from "not in this memtable —
// look in older SSTables". Do not collapse those two cases into one bool.
func (m *Memtable) Get(key []byte) (record, bool) {
	x := m.head
	for i := m.level - 1; i >= 0; i-- {
		for x.next[i] != nil && bytes.Compare(x.next[i].rec.key, key) < 0 {
			x = x.next[i]
		}
	}
	if n := x.next[0]; n != nil && bytes.Equal(n.rec.key, key) {
		return n.rec, true
	}
	return record{}, false
}

// ForEach calls fn for every record in ascending key order. This is how the
// memtable is flushed into a sorted SSTable.
func (m *Memtable) ForEach(fn func(record)) {
	for x := m.head.next[0]; x != nil; x = x.next[0] {
		fn(x.rec)
	}
}
