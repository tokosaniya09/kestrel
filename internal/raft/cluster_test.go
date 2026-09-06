package raft

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

type network struct {
	mu    sync.Mutex
	nodes map[int]*Raft
	down  map[int]bool
}

func newNetwork() *network {
	return &network{nodes: map[int]*Raft{}, down: map[int]bool{}}
}

func (n *network) add(id int, r *Raft) {
	n.mu.Lock()
	n.nodes[id] = r
	n.mu.Unlock()
}

func (n *network) setDown(id int, d bool) {
	n.mu.Lock()
	n.down[id] = d
	n.mu.Unlock()
}

func (n *network) isDown(id int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.down[id]
}

func (n *network) rpcRV(from, to int, a RequestVoteArgs) (RequestVoteReply, bool) {
	n.mu.Lock()
	if n.down[from] || n.down[to] {
		n.mu.Unlock()
		return RequestVoteReply{}, false
	}
	target := n.nodes[to]
	n.mu.Unlock()
	if target == nil {
		return RequestVoteReply{}, false
	}
	return target.RequestVote(a), true
}

func (n *network) rpcAE(from, to int, a AppendEntriesArgs) (AppendEntriesReply, bool) {
	n.mu.Lock()
	if n.down[from] || n.down[to] {
		n.mu.Unlock()
		return AppendEntriesReply{}, false
	}
	target := n.nodes[to]
	n.mu.Unlock()
	if target == nil {
		return AppendEntriesReply{}, false
	}
	return target.AppendEntries(a), true
}

func (n *network) rpcIS(from, to int, a InstallSnapshotArgs) (InstallSnapshotReply, bool) {
	n.mu.Lock()
	if n.down[from] || n.down[to] {
		n.mu.Unlock()
		return InstallSnapshotReply{}, false
	}
	target := n.nodes[to]
	n.mu.Unlock()
	if target == nil {
		return InstallSnapshotReply{}, false
	}
	return target.InstallSnapshot(a), true
}

type inmemTransport struct {
	net  *network
	from int
}

func (t *inmemTransport) SendRequestVote(to int, a RequestVoteArgs) (RequestVoteReply, bool) {
	return t.net.rpcRV(t.from, to, a)
}
func (t *inmemTransport) SendAppendEntries(to int, a AppendEntriesArgs) (AppendEntriesReply, bool) {
	return t.net.rpcAE(t.from, to, a)
}
func (t *inmemTransport) SendInstallSnapshot(to int, a InstallSnapshotArgs) (InstallSnapshotReply, bool) {
	return t.net.rpcIS(t.from, to, a)
}

type cluster struct {
	net        *network
	rafts      map[int]*Raft
	persisters map[int]Persister
	peers      []int
	n          int

	appliedMu sync.Mutex
	applied   map[int][]ApplyMsg
}

func makeCluster(n int) *cluster {
	net := newNetwork()
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i
	}
	c := &cluster{
		net:        net,
		rafts:      map[int]*Raft{},
		persisters: map[int]Persister{},
		peers:      peers,
		n:          n,
		applied:    map[int][]ApplyMsg{},
	}
	for i := 0; i < n; i++ {
		mp := NewMemoryPersister()
		c.persisters[i] = mp
		r := NewRaft(i, peers, &inmemTransport{net: net, from: i}, mp)
		net.add(i, r)
		c.rafts[i] = r
	}
	for id, r := range c.rafts {
		r.Start()
		go c.drainApplied(id, r)
	}
	return c
}

func (c *cluster) drainApplied(id int, r *Raft) {
	for m := range r.ApplyCh() {
		c.appliedMu.Lock()
		c.applied[id] = append(c.applied[id], m)
		c.appliedMu.Unlock()
	}
}

func (c *cluster) disconnect(id int) { c.net.setDown(id, true) }
func (c *cluster) reconnect(id int)  { c.net.setDown(id, false) }

func (c *cluster) crash(id int) {
	c.net.setDown(id, true)
	c.rafts[id].Stop()
}

func (c *cluster) restart(id int) {
	c.rafts[id].Stop()
	newRaft := NewRaft(id, c.peers, &inmemTransport{net: c.net, from: id}, c.persisters[id])
	c.net.add(id, newRaft)
	c.rafts[id] = newRaft
	newRaft.Start()
	go c.drainApplied(id, newRaft)
}

func (c *cluster) stopAll() {
	for _, r := range c.rafts {
		r.Stop()
	}
}

func (c *cluster) checkOneLeader(t *testing.T) int {
	t.Helper()
	for attempt := 0; attempt < 10; attempt++ {
		time.Sleep(200 * time.Millisecond)
		leadersByTerm := map[int][]int{}
		for id, r := range c.rafts {
			if c.net.isDown(id) {
				continue
			}
			term, isLeader := r.GetState()
			if isLeader {
				leadersByTerm[term] = append(leadersByTerm[term], id)
			}
		}
		lastTerm := -1
		for term := range leadersByTerm {
			if term > lastTerm {
				lastTerm = term
			}
		}
		if lastTerm >= 0 {
			if leaders := leadersByTerm[lastTerm]; len(leaders) == 1 {
				return leaders[0]
			} else if len(leaders) > 1 {
				t.Fatalf("term %d has %d leaders (must be at most one)", lastTerm, len(leaders))
			}
		}
	}
	t.Fatalf("expected one leader, found none after settling")
	return -1
}

func (c *cluster) checkNoLeader(t *testing.T) {
	t.Helper()
	time.Sleep(500 * time.Millisecond)
	for id, r := range c.rafts {
		if c.net.isDown(id) {
			continue
		}
		if _, isLeader := r.GetState(); isLeader {
			t.Fatalf("node %d is leader but no majority is reachable", id)
		}
	}
}

func (c *cluster) waitApplied(minCount int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := 0
		for id := range c.rafts {
			c.appliedMu.Lock()
			n := len(c.applied[id])
			c.appliedMu.Unlock()
			if n >= minCount {
				ready++
			}
		}
		if ready*2 > c.n {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func (c *cluster) appliedCount(id int) int {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	return len(c.applied[id])
}

// hasAppliedSnapshot reports whether node id's applied list already contains a
// snapshot message matching want. (drainApplied is the only consumer of a
// node's ApplyCh — reading it a second way, e.g. directly in a test, would race
// with that goroutine for each message; this checks the same recorded copy
// drainApplied already keeps instead.)
func (c *cluster) hasAppliedSnapshot(id int, want []byte) bool {
	c.appliedMu.Lock()
	defer c.appliedMu.Unlock()
	for _, m := range c.applied[id] {
		if m.IsSnapshot && bytes.Equal(m.Snapshot, want) {
			return true
		}
	}
	return false
}

func (c *cluster) dump(t *testing.T, label string) {
	t.Helper()
	for id := 0; id < c.n; id++ {
		term, role, logLen, commit, leaderID, snapIndex := c.rafts[id].DebugState()
		t.Logf("[%s] node %d: term=%d role=%s logLen=%d commitIndex=%d leaderID=%d snapshotIndex=%d down=%v",
			label, id, term, role, logLen, commit, leaderID, snapIndex, c.net.isDown(id))
	}
}
