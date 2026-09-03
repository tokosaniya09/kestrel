package raft

// This is your Phase 4 implementation file. Four methods, all running under the
// lock (their callers acquire r.mu for you), so you write pure state-machine
// logic — no goroutines, no locking. See PHASE4.md for the full walkthrough.

// startElection runs when the election timeout fires. It turns this node into a
// candidate for a new term and tries to win a majority of votes.
//
// Steps:
//  1. Under the lock: currentTerm++, role = Candidate, votedFor = own id,
//     resetElectionTimer(), and snapshot the new term + build a RequestVoteArgs.
//  2. Release the lock, then gather votes:
//        granted := 1 + r.requestVotesFromPeers(args)   // 1 = your own vote
//  3. Re-acquire the lock. Only become leader if you're STILL a candidate in the
//     SAME term you started (requestVotesFromPeers may have stepped you down on a
//     higher term, or a heartbeat may have arrived) AND granted isMajority.
//
// See PHASE4.md "Step 1 — startElection".
func (r *Raft) startElection() {
	r.mu.Lock()
	r.currentTerm++
	r.role = Candidate
	r.votedFor = r.id
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
//   - Otherwise grant iff you haven't voted this term, or already voted for THIS
//     candidate (votedFor == -1 || votedFor == args.CandidateID). On granting,
//     record votedFor and resetElectionTimer() (you've "heard from" the cluster).
//
// (The log-freshness check is a Phase 5 addition.) See PHASE4.md "Step 2".
func (r *Raft) handleRequestVote(args RequestVoteArgs) RequestVoteReply {
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}
	reply := RequestVoteReply{Term: r.currentTerm, VoteGranted: false}
	if args.Term < r.currentTerm {
		return reply // stale candidate
	}
	if r.votedFor == -1 || r.votedFor == args.CandidateID {
		r.votedFor = args.CandidateID
		r.role = Follower
		r.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply
}

// handleAppendEntries handles a heartbeat from a leader. Runs under the lock.
//
// Rules:
//   - If args.Term > currentTerm: becomeFollower(args.Term).
//   - Reply carries currentTerm.
//   - If args.Term < currentTerm: reply Success=false (reject a stale leader).
//   - Otherwise this is the legitimate leader for the term: set role = Follower,
//     record leaderID, resetElectionTimer(), reply Success=true. (Accepting a
//     current-term heartbeat is how a losing candidate reverts to follower.)
//
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
			if r.log[idx].Term != e.Term {
				r.log = r.log[:idx] // discard the conflicting entry and everything after
				r.log = append(r.log, args.Entries[i:]...)
				break
			}
			// same term at this index already — already have it, keep scanning
		} else {
			r.log = append(r.log, args.Entries[i:]...)
			break
		}
	}

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