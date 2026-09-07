package rpc

import (
	"net"
	"testing"
	"time"

	"kestrel/internal/raft"
)

// This test replicates commands over REAL TCP sockets on localhost — real
// gob serialization, real Accept/Dial, the works. Only the addresses are
// fake-ish (127.0.0.1 with OS-assigned ports); the network stack underneath
// is genuine.
func TestReplicationOverRealNetwork(t *testing.T) {
	const n = 3
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}

	addrs := map[int]string{}
	transport := NewRPCTransport(addrs) // one Transport, shared by every node —
	// a real transport has no notion of "from", unlike the in-memory fake,
	// since it just dials out; there's nothing per-node to configure.

	rafts := make([]*raft.Raft, n)
	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		r := raft.NewRaft(i, peers, transport, raft.NewMemoryPersister())
		rafts[i] = r

		listener, err := Serve(r, "127.0.0.1:0") // :0 = let the OS pick a free port
		if err != nil {
			t.Fatalf("Serve failed for node %d: %v", i, err)
		}
		listeners[i] = listener
		addrs[i] = listener.Addr().String() // visible to `transport` immediately — same map

		r.Start()
	}
	defer func() {
		for _, r := range rafts {
			r.Stop()
		}
		for _, l := range listeners {
			l.Close()
		}
	}()

	leader := waitForLeader(t, rafts)

	if _, _, isLeader := rafts[leader].Propose("hello-over-the-wire"); !isLeader {
		t.Fatalf("expected %d to still be leader", leader)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		allCaughtUp := true
		for _, r := range rafts {
			_, _, logLen, _, _, _ := r.DebugState()
			if logLen < 2 { // sentinel + our one real entry
				allCaughtUp = false
			}
		}
		if allCaughtUp {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("not every node replicated the command over the real network in time")
}

func waitForLeader(t *testing.T, rafts []*raft.Raft) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, r := range rafts {
			if _, isLeader := r.GetState(); isLeader {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected over the real network in time")
	return -1
}