package node

import "encoding/gob"

// CommandOp is the kind of mutation a Command performs.
type CommandOp int

const (
	OpPut CommandOp = iota
	OpDelete
)

// Command is what flows through Raft's log. LogEntry.Command is an
// interface{}, and gob needs to know every concrete type that can appear
// inside one — hence the registration below.
//
// ID distinguishes "my command" from "a different command that happened to
// land at the same log index". Propose returns an index, not a guarantee: if
// leadership changes before that index commits, another client's command can
// end up occupying it, so a caller that only checked the index would wrongly
// report success for a write that never happened.
type Command struct {
	ID    uint64
	Op    CommandOp
	Key   []byte
	Value []byte
}

func init() {
	gob.Register(Command{})
}
