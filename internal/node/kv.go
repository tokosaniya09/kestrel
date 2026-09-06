package node

import (
	"fmt"
	"time"
)

// This is your Phase 8 implementation file. Get is given (it's a trivial
// passthrough, deliberately — see PHASE8.md for why it's NOT linearizable in
// this simple form, and what that means). Put, Delete, and propose are yours:
// this is where a client's request gets correctly matched to its eventual
// commit, which is the real content of this phase.

const proposeTimeout = 2 * time.Second

// Get reads directly from this node's LOCAL storage engine. Provided as-is:
// there's nothing to implement, but there IS something to understand — see
// PHASE8.md "Why Get isn't linearizable (yet)".
func (n *Node) Get(key []byte) ([]byte, bool, error) {
	return n.db.Get(key)
}

// Put replicates a key/value write through Raft before returning.
//
func (n *Node) Put(key, value []byte) error {
	return n.propose(Command{ID: n.newCommandID(), Op: OpPut, Key: key, Value: value})
}

// Delete replicates a deletion through Raft before returning.
//
func (n *Node) Delete(key []byte) error {
	return n.propose(Command{ID: n.newCommandID(), Op: OpDelete, Key: key})
}

// propose is the shared engine behind Put and Delete: submit cmd to Raft, and
// don't return success until THIS SPECIFIC command (not just "something") has
// actually committed and been applied.
//
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