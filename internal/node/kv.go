package node

import (
	"fmt"
	"time"
)

// The node's key-value API: writes go through Raft, reads come from local
// state.

const proposeTimeout = 2 * time.Second

// Get reads directly from this node's local storage engine. It is fast and any
// node can serve it, but it is not linearizable: this node may not have applied
// a write that has already committed elsewhere. Use LinearizableGet when a
// stale read is unacceptable.
func (n *Node) Get(key []byte) ([]byte, bool, error) {
	return n.db.Get(key)
}

// Put replicates a key/value write through Raft before returning.
func (n *Node) Put(key, value []byte) error {
	return n.propose(Command{ID: n.newCommandID(), Op: OpPut, Key: key, Value: value})
}

// Delete replicates a deletion through Raft before returning.
func (n *Node) Delete(key []byte) error {
	return n.propose(Command{ID: n.newCommandID(), Op: OpDelete, Key: key})
}

// propose submits cmd to Raft and does not return success until that specific
// command — matched by ID, not merely by index — has committed and been
// applied. See Command's doc comment for why the ID check matters.
func (n *Node) propose(cmd Command) error {
	index, _, isLeader := n.raft.Propose(cmd)
	if !isLeader {
		return &NotLeaderError{LeaderHint: n.raft.Leader()}
	}

	ch := n.registerWait(index)
	select {
	case applied := <-ch:
		if applied.ID != cmd.ID {
			return fmt.Errorf("lost leadership before index %d committed — a different command landed there", index)
		}
		return nil
	case <-time.After(proposeTimeout):
		n.cancelWait(index)
		return fmt.Errorf("timed out waiting for index %d to commit", index)
	}
}