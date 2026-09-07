package client

import (
	"net"
	"testing"
	"time"

	"kestrel/internal/node"
	"kestrel/internal/raft"
	"kestrel/internal/rpc"
	"kestrel/internal/storage"
)

// This test file is package client (not client_test) deliberately, for one
// test specifically: TestClientFollowsRedirectFromWrongGuess needs to force a
// KNOWN wrong first guess to test the redirect path deterministically, rather
// than hoping round-robin happens to pick the wrong node on a given run.

type testCluster struct {
	rafts     []*raft.Raft
	dbs       []*storage.DB
	listeners []net.Listener
	addrs     map[int]string
}

func makeTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}

	addrs := map[int]string{}
	transport := rpc.NewRPCTransport(addrs)

	tc := &testCluster{addrs: addrs}
	for i := 0; i < n; i++ {
		db, err := storage.Open(t.TempDir())
		if err != nil {
			t.Fatalf("storage.Open: %v", err)
		}
		r := raft.NewRaft(i, peers, transport, raft.NewMemoryPersister())
		nd := node.NewNode(r, db)
		listener, err := rpc.ServeNode(r, nd, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("ServeNode: %v", err)
		}
		addrs[i] = listener.Addr().String()
		tc.rafts = append(tc.rafts, r)
		tc.dbs = append(tc.dbs, db)
		tc.listeners = append(tc.listeners, listener)
		r.Start()
	}
	t.Cleanup(func() {
		// Order matters: stop Raft and close listeners FIRST, so no apply loop
		// or in-flight RPC can touch a DB we're about to close. Only then
		// release the storage engines — on Windows an open file handle makes
		// t.TempDir()'s RemoveAll fail, which is exactly what this ordering
		// (and the previously-missing db.Close) prevents.
		for _, r := range tc.rafts {
			r.Stop()
		}
		for _, l := range tc.listeners {
			l.Close()
		}
		time.Sleep(50 * time.Millisecond) // let in-flight goroutines wind down
		for _, db := range tc.dbs {
			db.Close()
		}
	})
	return tc
}

func (tc *testCluster) waitForLeader(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for i, r := range tc.rafts {
			if _, isLeader := r.GetState(); isLeader {
				return i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return -1
}

// A client that starts knowing nothing about who's leader must still succeed
// at a full Put/Get/Delete round trip.
func TestClientPutGetDelete(t *testing.T) {
	tc := makeTestCluster(t, 3)
	tc.waitForLeader(t)

	c := New(tc.addrs)

	if err := c.Put([]byte("name"), []byte("toko")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	v, found, err := c.Get([]byte("name"))
	if err != nil || !found || string(v) != "toko" {
		t.Fatalf("Get: v=%q found=%v err=%v", v, found, err)
	}
	if err := c.Delete([]byte("name")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, found, err = c.Get([]byte("name"))
	if err != nil || found {
		t.Fatalf("expected key gone after Delete: found=%v err=%v", found, err)
	}
}

// Forcing a deliberately WRONG first guess (white-box, on purpose — this is
// the one place this test file needs unexported access) must still succeed,
// by following the redirect the wrong node hands back.
func TestClientFollowsRedirectFromWrongGuess(t *testing.T) {
	tc := makeTestCluster(t, 3)
	leaderID := tc.waitForLeader(t)
	var wrongGuess int
	for id := range tc.addrs {
		if id != leaderID {
			wrongGuess = id
			break
		}
	}

	c := New(tc.addrs)
	c.leader = wrongGuess // deterministic wrong guess

	if err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put should have succeeded after following the redirect: %v", err)
	}
	v, found, err := c.Get([]byte("k"))
	if err != nil || !found || string(v) != "v" {
		t.Fatalf("Get after redirect-following Put: v=%q found=%v err=%v", v, found, err)
	}
}

// The same Client instance, having cached a leader guess that's now dead,
// must recover and keep working after a real leader failover.
func TestClientSurvivesLeaderFailover(t *testing.T) {
	tc := makeTestCluster(t, 3)
	leaderID := tc.waitForLeader(t)

	c := New(tc.addrs)
	if err := c.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("initial Put failed: %v", err)
	}

	tc.rafts[leaderID].Stop()
	tc.listeners[leaderID].Close()

	deadline := time.Now().Add(5 * time.Second)
	newLeaderFound := false
	for time.Now().Before(deadline) && !newLeaderFound {
		for i, r := range tc.rafts {
			if i == leaderID {
				continue
			}
			if _, isLeader := r.GetState(); isLeader {
				newLeaderFound = true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !newLeaderFound {
		t.Fatal("no new leader elected after failover")
	}

	if err := c.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put after failover should have succeeded: %v", err)
	}
}