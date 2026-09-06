package node

import (
	"errors"
	"testing"
	"time"

	"kestrel/internal/raft"
	"kestrel/internal/storage"
)

// A small in-memory cluster, built only against raft's EXPORTED API (its own
// test harness lives in a _test.go file inside package raft, so it isn't
// importable from here — this is the unavoidable, if slightly repetitive,
// cost of Go's testing model). Each node pairs a real raft.Raft with a real
// storage.DB rooted in its own temp directory, wired together via NewNode.

type fakeTransport struct {
	nodes map[int]*raft.Raft
}

func (t *fakeTransport) SendRequestVote(to int, a raft.RequestVoteArgs) (raft.RequestVoteReply, bool) {
	target, ok := t.nodes[to]
	if !ok {
		return raft.RequestVoteReply{}, false
	}
	return target.RequestVote(a), true
}
func (t *fakeTransport) SendAppendEntries(to int, a raft.AppendEntriesArgs) (raft.AppendEntriesReply, bool) {
	target, ok := t.nodes[to]
	if !ok {
		return raft.AppendEntriesReply{}, false
	}
	return target.AppendEntries(a), true
}
func (t *fakeTransport) SendInstallSnapshot(to int, a raft.InstallSnapshotArgs) (raft.InstallSnapshotReply, bool) {
	target, ok := t.nodes[to]
	if !ok {
		return raft.InstallSnapshotReply{}, false
	}
	return target.InstallSnapshot(a), true
}

// makeTestCluster wires up n Nodes, each with its own real storage.DB (in a
// fresh t.TempDir()) and a Raft instance sharing one fakeTransport. Returns
// the Nodes and a helper to find the current leader.
func makeTestCluster(t *testing.T, n int) []*Node {
	t.Helper()
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	transport := &fakeTransport{nodes: map[int]*raft.Raft{}}

	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		db, err := storage.Open(t.TempDir())
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		r := raft.NewRaft(i, peers, transport, raft.NewMemoryPersister())
		transport.nodes[i] = r
		nodes[i] = NewNode(r, db)
		r.Start()
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.db.Close()
		}
	})
	return nodes
}

func waitForLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if _, isLeader := n.raft.GetState(); isLeader {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected in time")
	return nil
}

// A Put on the leader must eventually be readable — via Get — on every node in
// the cluster, since each has applied it to its own local storage engine.
func TestPutAndGet(t *testing.T) {
	nodes := makeTestCluster(t, 3)
	leader := waitForLeader(t, nodes)

	if err := leader.Put([]byte("name"), []byte("toko")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for _, n := range nodes {
		for {
			v, found, err := n.Get([]byte("name"))
			if err == nil && found && string(v) == "toko" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node never saw the replicated value (found=%v, err=%v)", found, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// A Put on a non-leader must fail with NotLeaderError, not silently no-op or
// hang.
func TestNotLeaderError(t *testing.T) {
	nodes := makeTestCluster(t, 3)
	leader := waitForLeader(t, nodes)

	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	err := follower.Put([]byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected an error putting to a non-leader")
	}
	var nle *NotLeaderError
	if !errors.As(err, &nle) {
		t.Fatalf("expected a *NotLeaderError, got %T: %v", err, err)
	}
}

// Delete must actually remove the key, cluster-wide.
func TestDeleteRemovesKey(t *testing.T) {
	nodes := makeTestCluster(t, 3)
	leader := waitForLeader(t, nodes)

	if err := leader.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := leader.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for _, n := range nodes {
		for {
			_, found, err := n.Get([]byte("k"))
			if err == nil && !found {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node still sees the deleted key")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}
