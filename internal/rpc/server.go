package rpc

import (
	"net"
	"net/rpc"

	"kestrel/internal/raft"
)

// RaftService adapts a *raft.Raft to the exact method shape Go's net/rpc
// package requires: exported methods of the form
// func (t *T) Method(args T1, reply *T2) error. Raft's own methods return
// their reply directly (e.g. RequestVote(args) RequestVoteReply) — the
// natural shape for the in-memory Transport every earlier phase's tests have
// used. This type is the (mechanical) adapter between that shape and what
// net/rpc needs to expose a Raft node over the network.
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

// Serve registers r as an RPC service and starts accepting connections at
// address (e.g. "127.0.0.1:9001", or "127.0.0.1:0" to let the OS pick a free
// port — useful in tests). Returns the listener so the caller can read the
// actual bound address (listener.Addr().String()) and shut it down later
// (listener.Close()).
func Serve(r *raft.Raft, address string) (net.Listener, error) {
	server := rpc.NewServer()
	if err := server.RegisterName("RaftService", &RaftService{raft: r}); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // listener was closed — normal shutdown
			}
			go server.ServeConn(conn)
		}
	}()
	return listener, nil
}