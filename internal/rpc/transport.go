package rpc

import (
	"net"
	"net/rpc"
	"time"

	"kestrel/internal/raft"
)

// RPCTransport dials out to peers over TCP using Go's net/rpc. addrs maps
// each peer's Raft id to its "host:port" — filled in by the caller (see
// rpc_test.go) once every node's listener is bound.
type RPCTransport struct {
	addrs map[int]string
}

// NewRPCTransport wraps addrs. Because Go maps are reference types, the
// caller can keep filling addrs in (e.g. as each node's listener comes up)
// even after constructing the Transport — every Send call reads the map live.
func NewRPCTransport(addrs map[int]string) *RPCTransport {
	return &RPCTransport{addrs: addrs}
}

// call dials peer, makes one RPC, and reports whether it succeeded.
//
// net/rpc has no DialTimeout of its own — the correct, idiomatic way to get a
// connection with a dial timeout is to compose net.DialTimeout (which returns
// a plain net.Conn) with rpc.NewClient (which wraps any connection satisfying
// io.ReadWriteCloser into an RPC client using the default gob codec).
func (t *RPCTransport) call(peer int, serviceMethod string, args, reply interface{}) bool {
	addr, ok := t.addrs[peer]
	if !ok {
		return false
	}
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false // peer unreachable — exactly like the in-memory fake's "down" case
	}
	client := rpc.NewClient(conn)
	defer client.Close()

	return client.Call(serviceMethod, args, reply) == nil
}

func (t *RPCTransport) SendRequestVote(peer int, args raft.RequestVoteArgs) (raft.RequestVoteReply, bool) {
	var reply raft.RequestVoteReply
	ok := t.call(peer, "RaftService.RequestVote", args, &reply)
	return reply, ok
}

func (t *RPCTransport) SendAppendEntries(peer int, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, bool) {
	var reply raft.AppendEntriesReply
	ok := t.call(peer, "RaftService.AppendEntries", args, &reply)
	return reply, ok
}

func (t *RPCTransport) SendInstallSnapshot(peer int, args raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, bool) {
	var reply raft.InstallSnapshotReply
	ok := t.call(peer, "RaftService.InstallSnapshot", args, &reply)
	return reply, ok
}