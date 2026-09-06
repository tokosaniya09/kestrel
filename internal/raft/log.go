package raft

// LogEntry is one command in the replicated log, tagged with the TERM of the
// leader that appended it. That term is what lets a node later detect and
// discard entries that came from a leader who never should have been in charge.
type LogEntry struct {
	Term    int
	Command interface{}
}

// ApplyMsg is either a normal committed entry OR a snapshot the state machine
// must install wholesale. Exactly one of the two shapes is populated:
//   - a command:  CommandIndex/Command set, IsSnapshot false
//   - a snapshot: IsSnapshot true, SnapshotIndex/SnapshotTerm/Snapshot set
//
// Only a node that RECEIVES a snapshot via InstallSnapshot (because it fell too
// far behind for normal replication) or LOADS one on restart gets one of these —
// calling Snapshot() locally does NOT emit one, since the caller (the state
// machine) already has that data; it's the one who just told Raft about it.
type ApplyMsg struct {
	CommandIndex int
	Command      interface{}

	IsSnapshot    bool
	SnapshotIndex int
	SnapshotTerm  int
	Snapshot      []byte
}

// The log is a SLICE whose position 0 does not necessarily represent raft-index
// 0 anymore. log[0] is always a sentinel entry — before any snapshot, it
// represents index 0 (Term 0, no command, exactly as in Phases 1-6); after
// Snapshot(index, ...) or receiving an InstallSnapshot, it represents whatever
// index was last snapshotted, with that entry's real term. r.snapshotIndex
// tracks which raft-index log[0] currently stands for — every other function in
// this file goes through logPos to translate, so callers elsewhere never need
// to know this offset exists.

// logPos converts a raft-log-index into a position in the log SLICE.
func (r *Raft) logPos(index int) int { return index - r.snapshotIndex }

func (r *Raft) lastLogIndex() int { return r.snapshotIndex + len(r.log) - 1 }

func (r *Raft) lastLogTerm() int { return r.log[len(r.log)-1].Term }

// termAt returns the term of the entry at index, or -1 if we don't have it —
// either because it's been compacted away by a snapshot, or because it's
// beyond the end of our log. Either way, -1 safely fails any consistency check
// that uses it, which is exactly the correct (if sometimes inefficient)
// behavior: never guess, only ever compare against data we actually hold.
func (r *Raft) termAt(index int) int {
	pos := r.logPos(index)
	if pos < 0 || pos >= len(r.log) {
		return -1
	}
	return r.log[pos].Term
}
