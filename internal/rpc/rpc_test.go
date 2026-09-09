package rpc

import (
	"net"
	"testing"
	"time"

	"kestrel/internal/node"
	"kestrel/internal/raft"
	"kestrel/internal/storage"
)

// Replicates a command over REAL TCP sockets on localhost — real gob
// serialization, real Accept/Dial. Only the addresses are synthetic
// (127.0.0.1 with OS-assigned ports); the network stack underneath is genuine.
func TestReplicationOverRealNetwork(t *testing.T) {
	const n = 3
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}

	// One Transport shared by every node: unlike the in-memory fake, a real
	// transport has no notion of "from" — it only dials out, so there is
	// nothing per-node to configure.
	addrs := map[int]string{}
	transport := NewRPCTransport(addrs)

	rafts := make([]*raft.Raft, n)
	dbs := make([]*storage.DB, n)
	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		db, err := storage.Open(t.TempDir())
		if err != nil {
			t.Fatalf("storage.Open for node %d: %v", i, err)
		}
		dbs[i] = db

		r := raft.NewRaft(i, peers, transport, raft.NewMemoryPersister())
		rafts[i] = r
		nd := node.NewNode(r, db)

		listener, err := ServeNode(r, nd, "127.0.0.1:0") // :0 = OS picks a free port
		if err != nil {
			t.Fatalf("ServeNode for node %d: %v", i, err)
		}
		listeners[i] = listener
		addrs[i] = listener.Addr().String() // same map the transport holds

		r.Start()
	}
	t.Cleanup(func() {
		// Stop Raft and close listeners before releasing the storage engines,
		// so no apply loop or in-flight RPC touches a DB that is closing. On
		// Windows an open handle also blocks t.TempDir()'s cleanup.
		for _, r := range rafts {
			r.Stop()
		}
		for _, l := range listeners {
			l.Close()
		}
		time.Sleep(50 * time.Millisecond)
		for _, db := range dbs {
			db.Close()
		}
	})

	leader := waitForLeader(t, rafts)

	cmd := node.Command{ID: 1, Op: node.OpPut, Key: []byte("k"), Value: []byte("v")}
	if _, _, isLeader := rafts[leader].Propose(cmd); !isLeader {
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
