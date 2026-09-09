package rpc

import (
	"errors"
	"net"
	"net/rpc"

	"kestrel/internal/node"
	"kestrel/internal/raft"
)

// PutArgs/PutReply, DeleteArgs/DeleteReply carry a structured LeaderHint
// instead of relying on a returned Go error to convey it — see the note on
// KVService below for why.
type PutArgs struct{ Key, Value []byte }
type PutReply struct {
	Success    bool
	LeaderHint int // valid when Success is false
}

type DeleteArgs struct{ Key []byte }
type DeleteReply struct {
	Success    bool
	LeaderHint int
}

// GetArgs carries a Linearizable flag: a plain read can be served by ANY node
// from local state (fast, possibly stale), while a linearizable read must go
// through the leader's ReadIndex protocol. GetReply therefore needs the same
// Success/LeaderHint shape as Put/Delete — a linearizable read CAN be
// redirected, unlike the plain kind.
type GetArgs struct {
	Key           []byte
	Linearizable  bool
}
type GetReply struct {
	Value      []byte
	Found      bool
	Success    bool
	LeaderHint int // valid when Success is false
}

// KVService exposes a *node.Node's Put/Get/Delete over RPC for EXTERNAL
// clients — as opposed to RaftService (Phase 9), which is for Raft nodes
// talking to each other internally.
//
// Put/Delete translate a *node.NotLeaderError into a STRUCTURED reply field
// (Success=false, LeaderHint=...) rather than returning it as this method's
// error. This is deliberate: net/rpc serializes errors as plain strings —
// the concrete *node.NotLeaderError type, and its LeaderHint field, would NOT
// survive the trip across the wire if returned as an error here. Putting the
// information in the reply payload instead sidesteps that limitation
// entirely, and mirrors how production RPC APIs generally separate "expected,
// actionable" failures (payload) from genuinely exceptional ones (transport
// error).
type KVService struct {
	node *node.Node
}

func (s *KVService) Put(args PutArgs, reply *PutReply) error {
	err := s.node.Put(args.Key, args.Value)
	if err == nil {
		reply.Success = true
		return nil
	}
	var nle *node.NotLeaderError
	if errors.As(err, &nle) {
		reply.Success = false
		reply.LeaderHint = nle.LeaderHint
		return nil // structured, expected failure — NOT an RPC-level error
	}
	return err // something genuinely unexpected — let this be a real RPC error
}

func (s *KVService) Delete(args DeleteArgs, reply *DeleteReply) error {
	err := s.node.Delete(args.Key)
	if err == nil {
		reply.Success = true
		return nil
	}
	var nle *node.NotLeaderError
	if errors.As(err, &nle) {
		reply.Success = false
		reply.LeaderHint = nle.LeaderHint
		return nil
	}
	return err
}

// Get serves either kind of read depending on args.Linearizable.
//
// A PLAIN read is answered by any node from local state — fast, no consensus,
// possibly slightly stale (Phase 8's deliberate tradeoff).
//
// A LINEARIZABLE read goes through node.LinearizableGet, which requires this
// node to be the leader and to prove it still is (Phase 11's ReadIndex). If
// it isn't, that comes back as a *node.NotLeaderError and is translated into
// the same structured redirect Put/Delete use — remember net/rpc serializes
// errors as plain strings, so the hint must travel in the payload.
func (s *KVService) Get(args GetArgs, reply *GetReply) error {
	var (
		value []byte
		found bool
		err   error
	)
	if args.Linearizable {
		value, found, err = s.node.LinearizableGet(args.Key)
	} else {
		value, found, err = s.node.Get(args.Key)
	}

	if err != nil {
		var nle *node.NotLeaderError
		if errors.As(err, &nle) {
			reply.Success = false
			reply.LeaderHint = nle.LeaderHint
			return nil // structured, expected — not an RPC-level error
		}
		return err
	}
	reply.Value = value
	reply.Found = found
	reply.Success = true
	return nil
}

// ServeNode registers BOTH RaftService (internal Raft RPCs) and KVService
// (client-facing RPCs) on ONE listener — a real node answers both kinds of
// calls on the single address it advertises to the rest of the cluster.
func ServeNode(r *raft.Raft, n *node.Node, address string) (net.Listener, error) {
	server := rpc.NewServer()
	if err := server.RegisterName("RaftService", &RaftService{raft: r}); err != nil {
		return nil, err
	}
	if err := server.RegisterName("KVService", &KVService{node: n}); err != nil {
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
				return
			}
			go server.ServeConn(conn)
		}
	}()
	return listener, nil
}
