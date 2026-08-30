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

// With only a minority reachable, no leader can be elected.
func TestNoLeaderWithoutMajority(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	c.checkOneLeader(t)

	// Take down two of three: no node can reach a majority.
	c.disconnect(0)
	c.disconnect(1)

	c.checkNoLeader(t)
}
