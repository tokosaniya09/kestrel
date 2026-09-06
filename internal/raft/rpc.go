package raft

// The RPCs Raft needs. AppendEntries carries real log entries (Phase 5) and
// RequestVote's log-freshness fields are checked (Phase 5). InstallSnapshot
// (Phase 7) is how a leader catches up a follower whose needed log entries have
// already been compacted away by a snapshot.

type RequestVoteArgs struct {
	Term        int
	CandidateID int

	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// InstallSnapshotArgs carries an ENTIRE snapshot in one shot. Real systems
// chunk large snapshots across several RPCs (the Raft paper's version streams
// it in pieces) — we send it whole for simplicity, which is fine at the scale
// this project operates at, and is a known, flagged simplification.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply carries only Term — unlike AppendEntries, there's no
// success/failure semantics: a follower that receives a well-formed snapshot
// from a leader at a valid term simply accepts it.
type InstallSnapshotReply struct {
	Term int
}

// Transport is how a node sends RPCs to its peers. The bool reports whether the
// RPC was delivered at all (false = peer unreachable).
type Transport interface {
	SendRequestVote(to int, args RequestVoteArgs) (RequestVoteReply, bool)
	SendAppendEntries(to int, args AppendEntriesArgs) (AppendEntriesReply, bool)
	SendInstallSnapshot(to int, args InstallSnapshotArgs) (InstallSnapshotReply, bool)
}
