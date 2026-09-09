package raft

// The leader's replication decisions: what to send each follower, and when an
// entry counts as committed. All three methods run under r.mu, held by their
// caller (broadcastReplication in raft.go), and none of them send RPCs.
//
// The follower side — appending entries, truncating conflicts, advancing its
// own commitIndex — lives in handleAppendEntries in election.go.

// buildAppendEntriesArgs constructs what to send to peer, based on how much of
// the log peer is believed to have (r.nextIndex[peer]).
//
func (r *Raft) buildAppendEntriesArgs(peer int) AppendEntriesArgs {
	prevIndex := r.nextIndex[peer] - 1
	prevTerm := r.termAt(prevIndex)
	entries := append([]LogEntry(nil), r.log[r.logPos(r.nextIndex[peer]):]...) // copy
	return AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}
}

// handleAppendEntriesReply processes a follower's response. args is exactly what
// you sent it; reply is what came back. The caller (broadcastAppendEntries) has
// already handled a higher-term reply by stepping this node down BEFORE calling
// you — you can assume the term situation is normal.
//
func (r *Raft) handleAppendEntriesReply(peer int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if r.role != Leader || args.Term != r.currentTerm {
		return // stale reply for a round we've since moved past
	}
	if reply.Success {
		r.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		r.nextIndex[peer] = r.matchIndex[peer] + 1
		r.tryAdvanceCommitIndex()
		return
	}
	if r.nextIndex[peer] > 1 {
		r.nextIndex[peer]--
	}
}
// tryAdvanceCommitIndex looks for the highest index N > commitIndex that a
// majority of matchIndex (including this leader itself, via matchIndex[r.id])
// has reached, AND whose entry belongs to the CURRENT term — then sets
// commitIndex = N.
//
// The current-term restriction is the subtle safety rule from the Raft paper
// (§5.4.2): a leader must never declare an OLDER-term entry committed purely by
// counting replicas of it — only once one of the leader's OWN entries reaches a
// majority does that transitively confirm everything before it is safe too.
//
func (r *Raft) tryAdvanceCommitIndex() {
	for n := r.lastLogIndex(); n > r.commitIndex; n-- {
		if r.termAt(n) != r.currentTerm {
			continue // §5.4.2 — never commit an older-term entry by counting alone
		}
		count := 0
		for _, peer := range r.peers {
			if r.matchIndex[peer] >= n {
				count++
			}
		}
		if r.isMajority(count) {
			r.commitIndex = n
			return
		}
	}
}
