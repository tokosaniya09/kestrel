package node

import "encoding/gob"

// CommandOp is the kind of mutation a Command performs.
type CommandOp int

const (
	OpPut CommandOp = iota
	OpDelete
)

// Command is what actually flows through Raft's log now — replacing the plain
// strings your raft package's own tests used as a stand-in. This is exactly
// the concrete type Phase 6's gob.Register gotcha was warning you about:
// LogEntry.Command is `interface{}`, and gob needs to know every concrete type
// that might show up inside it.
//
// ID exists for one specific reason: to tell "my command" apart from "a
// different command that happened to land at the same log index" once
// leadership changes are in the mix. See PHASE8.md for why index alone isn't
// enough.
type Command struct {
	ID    uint64
	Op    CommandOp
	Key   []byte
	Value []byte
}

func init() {
	gob.Register(Command{})
}
