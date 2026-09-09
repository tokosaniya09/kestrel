package raft

import "time"

// The ReadIndex protocol, which makes linearizable reads possible without
// writing to the log. ReadIndex takes r.mu itself, since it must release the
// lock partway through to send heartbeats.

const readIndexTimeout = 2 * time.Second

// ErrNotLeader is returned by ReadIndex when this node isn't the leader, or
// discovers mid-protocol that it no longer is.
type readIndexError string

func (e readIndexError) Error() string { return string(e) }

const (
	ErrNotLeader        = readIndexError("not the leader")
	ErrReadIndexTimeout = readIndexError("timed out establishing a read index")
)

// ReadIndex returns only once this node has proven it can serve a linearizable
// read: that its state machine reflects everything committed before the read
// began. The caller then reads local state normally — the guarantee comes from
// having waited here first, not from anything special about the read itself.
//
// Two things can make a local read stale, and each step addresses one. A
// follower may not have applied a committed entry yet, so we wait for
// lastApplied to reach the commitIndex captured at the start. And a
// partitioned leader has no way to learn it has been deposed, so it would
// happily serve state frozen at the moment of the partition — confirming a
// fresh majority ack rules that out, since any two majorities overlap and a
// node at a higher term rejects the heartbeat rather than acking it.
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