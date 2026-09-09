package node

import (
	"fmt"
	"sync"
	"sync/atomic"

	"kestrel/internal/raft"
	"kestrel/internal/storage"
)

// Node is one cluster member: a Raft instance for consensus, paired with its
// own private copy of the Kestrel storage engine as its state machine. Every
// node in a cluster has its own independent Node — there is no shared storage;
// Raft's whole job is making sure every Node's local DB ends up with the same
// data, applied in the same order.
type Node struct {
	raft *raft.Raft
	db   *storage.DB

	mu      sync.Mutex
	pending map[int]chan Command // log index -> channel delivering whatever Command actually lands there

	nextID uint64 // monotonic counter for Command.ID
}

// NewNode wires a Raft instance to a local storage engine and starts the
// background loop that applies committed commands to it. Call raft.Start()
// yourself, before or after this — NewNode only starts ITS OWN loop.
func NewNode(r *raft.Raft, db *storage.DB) *Node {
	n := &Node{raft: r, db: db, pending: map[int]chan Command{}}
	go n.applyLoop()
	return n
}

// applyLoop drains committed entries from Raft and applies them to the local
// storage engine — the actual "state machine" half of state-machine
// replication. Provided: this is mechanical dispatch, the same category as
// Raft's own applyLoop.
func (n *Node) applyLoop() {
	for msg := range n.raft.ApplyCh() {
		if msg.IsSnapshot {
			// Installing a received snapshot into the local storage engine is
			// a later phase's problem — for now, an incoming snapshot just
			// means "trust that your log will replay correctly from here,"
			// which is already true since storage.DB persists everything
			// itself. Flagged as a known gap in PHASE8.md.
			continue
		}

		cmd, ok := msg.Command.(Command)
		if !ok {
			continue // shouldn't happen once every Propose call uses Command
		}

		switch cmd.Op {
		case OpPut:
			n.db.Put(cmd.Key, cmd.Value)
		case OpDelete:
			n.db.Delete(cmd.Key)
		}

		n.mu.Lock()
		if ch, exists := n.pending[msg.CommandIndex]; exists {
			ch <- cmd
			delete(n.pending, msg.CommandIndex)
		}
		n.mu.Unlock()
	}
}

// registerWait creates the channel a caller blocks on to learn what command
// (if any specific one) ends up applied at index. Provided.
func (n *Node) registerWait(index int) chan Command {
	ch := make(chan Command, 1)
	n.mu.Lock()
	n.pending[index] = ch
	n.mu.Unlock()
	return ch
}

// cancelWait removes a registered wait, e.g. after a timeout — so applyLoop
// doesn't block forever trying to send to a channel nobody's reading anymore
// (it's buffered size 1, so it wouldn't actually block, but the entry would
// leak in the map forever otherwise). Provided.
func (n *Node) cancelWait(index int) {
	n.mu.Lock()
	delete(n.pending, index)
	n.mu.Unlock()
}

func (n *Node) newCommandID() uint64 {
	return atomic.AddUint64(&n.nextID, 1)
}

// NotLeaderError is returned by Put/Delete when this node isn't the leader.
// LeaderHint is this node's best current guess at who is (or -1 if unknown) —
// a real client/RPC layer would use it to retry elsewhere (a later phase).
type NotLeaderError struct {
	LeaderHint int
}

func (e *NotLeaderError) Error() string {
	if e.LeaderHint < 0 {
		return "not the leader, and no leader is currently known"
	}
	return fmt.Sprintf("not the leader — try node %d", e.LeaderHint)
}

// raftErrNotLeader mirrors raft's not-leader sentinel so LinearizableGet can
// distinguish "ask someone else" from a genuine failure.
var raftErrNotLeader = raft.ErrNotLeader
