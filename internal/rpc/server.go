package rpc

import "kestrel/internal/raft"

// RaftService exposes a *raft.Raft over net/rpc, which requires exported
// methods of the form func (t *T) Method(args T1, reply *T2) error. Raft's own
// methods return their reply directly (e.g. RequestVote(args) RequestVoteReply),
// so this type is the adapter between the two shapes.
//
// These are the internal RPCs cluster members use to talk to each other;
// KVService (kvservice.go) is the separate, client-facing surface. Both are
// registered on one listener by ServeNode.
type RaftService struct {
	raft *raft.Raft
}

func (s *RaftService) RequestVote(args raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	*reply = s.raft.RequestVote(args)
	return nil
}

func (s *RaftService) AppendEntries(args raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	*reply = s.raft.AppendEntries(args)
	return nil
}

func (s *RaftService) InstallSnapshot(args raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	*reply = s.raft.InstallSnapshot(args)
	return nil
}
