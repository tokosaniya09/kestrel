package raft

// This is your Phase 6 implementation file: ONE method. Runs under r.mu (every
// caller already holds the lock) — you write pure logic, no goroutines, no I/O
// concerns beyond calling the provided persister.
//
// You are NOT responsible for deciding WHERE persist() gets called from
// raft.go's provided code — becomeFollower and Propose already call it, since
// they live in files you don't own this phase. You WILL find three more call
// sites to add yourself, in your own election.go (see PHASE6.md "Step 2") —
// that's the real lesson here: knowing exactly which state changes must hit
// disk before Raft can safely reply to an RPC.
//
// See PHASE6.md "Step 1 — persist".
func (r *Raft) persist() {
	data, err := encodeState(r.currentTerm, r.votedFor, r.log)
	if err != nil {
		panic(err) // encoding our own in-memory state should never fail
	}
	if err := r.persister.Save(data); err != nil {
		panic(err)
	}
}
