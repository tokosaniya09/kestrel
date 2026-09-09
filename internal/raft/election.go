package raft

// Leader election and the follower-side RPC handlers. Every method here runs
// under r.mu — startElection takes the lock itself (and must release it before
// sending RPCs); the handlers are called with it already held.

// startElection runs when the election timeout fires. It turns this node into a
// candidate for a new term and tries to win a majority of votes.
//
// The re-check after gathering votes is essential: the lock is released while
// RPCs are in flight, so by the time they return this node may have been
// stepped down by a higher term or accepted another leader's heartbeat.
// Becoming leader without re-checking could produce two leaders in one term.
func (r *Raft) startElection() {
	r.mu.Lock()
	r.currentTerm++
	r.role = Candidate
	r.votedFor = r.id
	r.persist() // Phase 6: durably record the new term/vote BEFORE anyone hears
	// about it — if we crash right after unlocking but before any reply comes
	// back, a restart must still remember we already voted for ourselves here.
	r.resetElectionTimer()
	term := r.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  r.id,
		LastLogIndex: r.lastLogIndex(),
		LastLogTerm:  r.lastLogTerm(),
	}
	r.mu.Unlock() // <-- release BEFORE sending RPCs

	granted := 1 + r.requestVotesFromPeers(args) // 1 = our own vote

	r.mu.Lock()
	defer r.mu.Unlock()
	// Only take power if we're still the candidate we were when we started:
	// a higher-term reply or an incoming heartbeat may have changed things.
	if r.role == Candidate && r.currentTerm == term && r.isMajority(granted) {
		r.becomeLeader()
	}
}

// becomeLeader promotes this node to leader. Assumes mu held. (Phase 5 will also
// initialize per-peer log bookkeeping here.)
func (r *Raft) becomeLeader() {
	r.role = Leader
	r.leaderID = r.id
	r.nextIndex = map[int]int{}
	r.matchIndex = map[int]int{}
	for _, p := range r.peers {
		r.nextIndex[p] = r.lastLogIndex() + 1
		r.matchIndex[p] = 0
	}
	r.matchIndex[r.id] = r.lastLogIndex()
}

// handleRequestVote decides whether to grant a vote. Runs under the lock.
//
// Rules:
//   - If args.Term > currentTerm: becomeFollower(args.Term) first (a higher term
//     always wins).
//   - Reply carries currentTerm.
//   - If args.Term < currentTerm: deny (stale candidate).
//   - The candidate's log must be at least as up to date as ours (Phase 5):
//     a higher LastLogTerm wins outright; on a tie, a longer/equal log wins.
//     This is what stops a node whose log fell behind from ever becoming leader.
//   - Otherwise grant iff you haven't voted this term, or already voted for THIS
//     candidate (votedFor == -1 || votedFor == args.CandidateID). On granting,
//     record votedFor and resetElectionTimer() (you've "heard from" the cluster).
//
// The log-freshness check is what prevents a node whose log has fallen behind
// from winning an election and overwriting committed history.
func (r *Raft) handleRequestVote(args RequestVoteArgs) RequestVoteReply {
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}
	reply := RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	if args.Term < r.currentTerm {
		return reply // stale candidate
	}

	upToDate := args.LastLogTerm > r.lastLogTerm() ||
		(args.LastLogTerm == r.lastLogTerm() && args.LastLogIndex >= r.lastLogIndex())

	if (r.votedFor == -1 || r.votedFor == args.CandidateID) && upToDate {
		r.votedFor = args.CandidateID
		r.persist() // Phase 6: must survive a crash — otherwise a restart could
		// grant a second, conflicting vote in a term we already voted in
		r.role = Follower
		r.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply
}

// handleAppendEntries handles a heartbeat/replication call from a leader. Runs
// under the lock.
//
// Rules:
//   - If args.Term > currentTerm: becomeFollower(args.Term).
//   - Reply carries currentTerm.
//   - If args.Term < currentTerm: reply Success=false (reject a stale leader).
//   - Otherwise this is the legitimate leader for the term: set role = Follower,
//     record leaderID, resetElectionTimer().
//   - Consistency check: our log must contain PrevLogIndex with PrevLogTerm, or
//     we refuse (Success stays false) and the leader backs up nextIndex and
//     retries.
//   - Append new entries, truncating at the first conflict (same index,
//     different term) and leaving already-matching entries alone.
//   - Advance our own commitIndex from LeaderCommit, capped at what we actually
//     have.
func (r *Raft) handleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}
	reply := AppendEntriesReply{Term: r.currentTerm, Success: false}
	if args.Term < r.currentTerm {
		return reply // stale leader
	}

	r.role = Follower
	r.leaderID = args.LeaderID
	r.resetElectionTimer()

	// Consistency check: our log must contain PrevLogIndex with PrevLogTerm.
	if args.PrevLogIndex > r.lastLogIndex() || r.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		return reply // Success stays false; leader will back up nextIndex and retry
	}

	// Append new entries, truncating at the first conflict (same index, different
	// term) and leaving already-matching entries alone.
	insertAt := args.PrevLogIndex + 1
	for i, e := range args.Entries {
		idx := insertAt + i
		if idx <= r.lastLogIndex() {
			if r.log[r.logPos(idx)].Term != e.Term {
				r.log = r.log[:r.logPos(idx)] // discard the conflicting entry and everything after
				r.log = append(r.log, args.Entries[i:]...)
				break
			}
			// same term at this index already — already have it, keep scanning
		} else {
			r.log = append(r.log, args.Entries[i:]...)
			break
		}
	}
	r.persist() // Phase 6: the log may have changed — must survive a crash
	// before we ack. (Unconditional here for simplicity — even a pure
	// heartbeat with no actual change re-persists the whole log. That's
	// correct but wasteful; only persisting when the log truly changed is a
	// good stretch goal once this is working.)

	if args.LeaderCommit > r.commitIndex {
		newCommit := args.LeaderCommit
		if last := r.lastLogIndex(); newCommit > last {
			newCommit = last // never commit past what we actually have
		}
		r.commitIndex = newCommit
	}

	reply.Success = true
	return reply
}
