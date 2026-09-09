package node

import "errors"

// LinearizableGet serves a read that reflects every write which completed
// before this call started — the guarantee plain Get does not provide.
//
// The order matters: ReadIndex must return successfully before the DB is
// touched. Reading first and confirming afterward would prove nothing, since
// the value could already have been stale when it was read.
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
