package node

import "errors"

// LinearizableGet serves a read that reflects every write which completed
// before this call started — the guarantee plain Get deliberately does not
// provide (see PHASE8.md).
//
// Provided: the interesting work is in raft.ReadIndex (your Phase 11 task).
// This is just the wiring — establish the read index, then read local state.
// Note the ORDER: ReadIndex must return successfully BEFORE we touch the DB.
// Reading first and confirming afterward would prove nothing, since the value
// could have been stale at the moment we read it.
func (n *Node) LinearizableGet(key []byte) ([]byte, bool, error) {
	if err := n.raft.ReadIndex(); err != nil {
		// Not the leader (or lost leadership mid-protocol) — surface it in the
		// same shape Put/Delete use, so the client's existing redirect logic
		// applies unchanged.
		if errors.Is(err, raftErrNotLeader) {
			return nil, false, &NotLeaderError{LeaderHint: n.raft.Leader()}
		}
		return nil, false, err
	}
	return n.db.Get(key)
}
