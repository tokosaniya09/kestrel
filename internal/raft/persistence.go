package raft

// persist writes the durable half of Raft's state — currentTerm, votedFor, the
// log, and any snapshot — through the Persister. Callers already hold r.mu.
//
// It is called from every point that mutates that state: becomeFollower and
// Propose (raft.go), and startElection, handleRequestVote, and
// handleAppendEntries (election.go). Each of those must reach disk before Raft
// replies to an RPC, or a crash could let the node contradict itself on
// restart — voting twice in one term, or losing an entry it already
// acknowledged.
//
// A failure here panics deliberately: a node that cannot record its own
// safety-critical state cannot safely keep participating.
func (r *Raft) persist() {
	data, err := encodeState(r.currentTerm, r.votedFor, r.log, r.snapshotIndex, r.snapshotTerm, r.snapshotData)
	if err != nil {
		panic(err)
	}
	if err := r.persister.Save(data); err != nil {
		panic(err)
	}
}