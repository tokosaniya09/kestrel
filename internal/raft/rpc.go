package raft

// The RPCs Raft needs. AppendEntries now carries real log entries (Phase 4 only
// used it as an empty heartbeat), and RequestVote's log-freshness fields are now
// actually read.

type RequestVoteArgs struct {
	Term        int // candidate's term
	CandidateID int // who is asking for the vote

	// Log freshness: a voter must refuse a candidate whose log is behind its own.
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int  // responder's term, so a stale candidate can catch up
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderID int

	// Consistency check: the follower must have an entry at PrevLogIndex whose
	// term is PrevLogTerm, or it refuses this call (the "log matching" rule).
	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntry // empty = pure heartbeat
	LeaderCommit int        // leader's commitIndex, so followers can advance theirs
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Transport is how a node sends RPCs to its peers. The bool reports whether the
// RPC was delivered at all (false = peer unreachable).
type Transport interface {
	SendRequestVote(to int, args RequestVoteArgs) (RequestVoteReply, bool)
	SendAppendEntries(to int, args AppendEntriesArgs) (AppendEntriesReply, bool)
}
