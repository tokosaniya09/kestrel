package raft

// The two RPCs Raft needs for leader election.
//
// RequestVote is sent by a candidate to gather votes. AppendEntries is sent by a
// leader; in this phase it carries no log entries and serves only as a heartbeat
// that keeps followers from starting their own elections. (Log entries arrive in
// Phase 5.)

type RequestVoteArgs struct {
	Term        int // candidate's term
	CandidateID int // who is asking for the vote

	// Log freshness fields — unused until Phase 5, but wired in now so the RPC
	// shape doesn't change later. Leave them zero for Phase 4.
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int  // responder's term, so a stale candidate can catch up
	VoteGranted bool // did the responder vote for the candidate?
}

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderID int // so followers can record who the leader is
	// Entries + commit info arrive in Phase 5. Empty = heartbeat.
}

type AppendEntriesReply struct {
	Term    int  // responder's term
	Success bool // true if the responder accepted this leader
}

// Transport is how a node sends RPCs to its peers. The bool reports whether the
// RPC was delivered at all (false = peer unreachable). The real implementation is
// swapped in later; tests use an in-memory network.
type Transport interface {
	SendRequestVote(to int, args RequestVoteArgs) (RequestVoteReply, bool)
	SendAppendEntries(to int, args AppendEntriesArgs) (AppendEntriesReply, bool)
}
