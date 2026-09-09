package raft

import "time"

// This is your Phase 11 implementation file: ONE method, and it takes its own
// lock (unlike election.go/replication.go/snapshot.go, whose callers lock for
// them) — because it needs to RELEASE the lock partway through to send
// heartbeats, the same discipline as startElection.
//
// See PHASE11.md for the full walkthrough.

const readIndexTimeout = 2 * time.Second

// ErrNotLeader is returned by ReadIndex when this node isn't the leader, or
// discovers mid-protocol that it no longer is.
type readIndexError string

func (e readIndexError) Error() string { return string(e) }

const (
	ErrNotLeader    = readIndexError("not the leader")
	ErrReadIndexTimeout = readIndexError("timed out establishing a read index")
)

// ReadIndex implements the ReadIndex protocol: it returns only once this node
// has PROVEN it can serve a linearizable read, i.e. once it has established
// that its state machine reflects everything committed before the read began.
//
// The caller (node.Node.Get, once you wire it up) then reads its local state
// machine normally — the guarantee comes from having waited here first, not
// from anything special about the read itself.
//
func (r *Raft) ReadIndex() error {
	r.mu.Lock()
	if r.role != Leader {
		r.mu.Unlock()
		return ErrNotLeader
	}
	term := r.currentTerm
	readIndex := r.commitIndex
	r.mu.Unlock() // release BEFORE the heartbeat round

	if !r.confirmStillLeader(term) {
		return ErrNotLeader
	}

	if !r.waitForApplied(readIndex, readIndexTimeout) {
		return ErrReadIndexTimeout
	}
	return nil
}