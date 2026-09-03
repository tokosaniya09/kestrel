package raft

import (
	"testing"
)

// A fresh 3-node cluster must elect exactly one leader.
func TestInitialElection(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)

	// Everyone should agree on a term >= 1 (an election happened).
	term, _ := c.rafts[leader].GetState()
	if term < 1 {
		t.Fatalf("leader term is %d, expected >= 1", term)
	}
}

// If the leader crashes, the rest must elect a new one.
func TestReElectionAfterLeaderFails(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader1 := c.checkOneLeader(t)
	c.crash(leader1)

	leader2 := c.checkOneLeader(t)
	if leader2 == leader1 {
		t.Fatalf("crashed leader %d cannot be the new leader", leader1)
	}
}

// With only a minority reachable, no leader can be elected — and if a leader
// already existed, it becomes unable to commit anything (though, without a
// CheckQuorum extension, it won't necessarily know it's lost quorum).
func TestNoLeaderWithoutMajority(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)

	// Disconnect the LEADER itself plus one follower, so the single node left
	// standing is guaranteed to be a plain follower that never won an election.
	// (Hardcoding two fixed ids here would be wrong: election timeouts are
	// randomized, so the winner varies run to run. If the winner happened to be
	// the one node left connected, it would correctly still report itself as
	// leader — Raft leaders don't auto-detect lost quorum — and this test would
	// wrongly fail on a correct implementation.)
	c.disconnect(leader)
	remaining := 1
	for id := range c.rafts {
		if id == leader {
			continue
		}
		if remaining == 0 {
			break
		}
		c.disconnect(id)
		remaining--
	}

	c.checkNoLeader(t)
}
