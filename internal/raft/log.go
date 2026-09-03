package raft

// LogEntry is one command in the replicated log, tagged with the TERM of the
// leader that appended it. That term is what lets a node later detect and
// discard entries that came from a leader who never should have been in charge.
type LogEntry struct {
	Term    int
	Command interface{}
}

// ApplyMsg is a committed entry handed to the state machine. A later phase wires
// this into the KV store; for now, tests just read it off ApplyCh().
type ApplyMsg struct {
	CommandIndex int
	Command      interface{}
}

// The log is 1-INDEXED: log[0] is a sentinel (Term 0, no command). That makes
// "no real entries yet" naturally give lastLogIndex() == 0, lastLogTerm() == 0 —
// no special-casing needed at the boundaries.

func (r *Raft) lastLogIndex() int { return len(r.log) - 1 }

func (r *Raft) lastLogTerm() int { return r.log[r.lastLogIndex()].Term }

// termAt returns the term of the entry at index, or -1 if index is out of range.
func (r *Raft) termAt(index int) int {
	if index < 0 || index >= len(r.log) {
		return -1
	}
	return r.log[index].Term
}
