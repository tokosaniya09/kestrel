package raft

import (
	"testing"
	"time"
)

// A leader that proposes commands should get them applied, in order, on a
// majority of nodes — and every node that applies a given index must agree on
// what command is there.
func TestLogReplication(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)

	cmds := []string{"set x 1", "set y 2", "set z 3"}
	for _, cmd := range cmds {
		if _, _, isLeader := c.rafts[leader].Propose(cmd); !isLeader {
			t.Fatalf("node %d should still be leader", leader)
		}
	}

	if !c.waitApplied(len(cmds), 3*time.Second) {
		t.Fatal("commands were not applied to a majority in time")
	}

	for i := 1; i <= len(cmds); i++ {
		var want string
		have := false
		for id := range c.rafts {
			c.appliedMu.Lock()
			entries := append([]ApplyMsg(nil), c.applied[id]...)
			c.appliedMu.Unlock()
			for _, m := range entries {
				if m.CommandIndex != i {
					continue
				}
				got := m.Command.(string)
				if !have {
					want, have = got, true
				} else if got != want {
					t.Fatalf("nodes disagree on command at index %d: %q vs %q", i, want, got)
				}
			}
		}
		if !have {
			t.Fatalf("no node applied index %d", i)
		}
	}
}

// Without a majority reachable, a leader can still accept Proposes locally, but
// nothing may ever be committed or applied.
func TestNoCommitWithoutMajority(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)
	for id := range c.rafts {
		if id != leader {
			c.disconnect(id) // isolates both followers from everyone, leader included
		}
	}

	if _, _, isLeader := c.rafts[leader].Propose("orphan-command"); !isLeader {
		t.Fatalf("node %d should still consider itself leader", leader)
	}

	if c.waitApplied(1, 1*time.Second) {
		t.Fatal("a command was applied without majority replication")
	}
}

// A follower that misses some entries while disconnected must catch up once
// it's reconnected, via the leader backing up nextIndex and retrying.
func TestFollowerCatchesUpAfterPartition(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)
	c.dump(t, "initial leader elected")
	var laggard int
	for id := range c.rafts {
		if id != leader {
			laggard = id
			break
		}
	}
	c.disconnect(laggard)

	for _, cmd := range []string{"a", "b", "c"} {
		if _, _, isLeader := c.rafts[leader].Propose(cmd); !isLeader {
			t.Fatalf("expected %d to still be leader", leader)
		}
	}
	// Leader + the one remaining follower are still a majority of 3.
	if !c.waitApplied(3, 3*time.Second) {
		c.dump(t, "waitApplied timed out")
		t.Fatal("commands should commit via leader + the one remaining follower")
	}
	c.dump(t, "before reconnect")

	c.reconnect(laggard)

	// NOTE: while isolated, the laggard's own election timeout may have fired
	// one or more times. Each attempt bumps its currentTerm even though the
	// vote never reaches anyone (it's cut off) — so its term can end up higher
	// than the rest of the cluster's, invisibly. The moment it reconnects, that
	// inflated term becomes live: the next exchange with it can force the
	// CURRENT, perfectly healthy leader to step down (per "a higher term always
	// wins"), even though nothing was wrong. This is a known, real Raft
	// phenomenon — a rejoining partitioned node disrupting a healthy cluster —
	// which production Raft addresses with a "PreVote" phase (a candidate checks
	// it could plausibly win BEFORE bumping its term). We haven't built that
	// (a good stretch goal), so we just give the cluster room to re-settle.
	c.checkOneLeader(t)
	c.dump(t, "after post-reconnect re-election settled")

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && c.appliedCount(laggard) < 3 {
		time.Sleep(50 * time.Millisecond)
	}
	c.dump(t, "final state")
	if got := c.appliedCount(laggard); got < 3 {
		t.Fatalf("node %d never caught up after reconnecting: applied %d/3", laggard, got)
	}
}