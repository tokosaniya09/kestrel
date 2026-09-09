package raft

import (
	"testing"
	"time"
)

// A leader with a healthy majority must be able to establish a read index.
func TestReadIndexSucceedsOnLeader(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)
	if err := c.rafts[leader].ReadIndex(); err != nil {
		t.Fatalf("ReadIndex on a healthy leader should succeed, got: %v", err)
	}
}

// A follower must refuse — only the leader can serve a linearizable read.
func TestReadIndexRejectedOnFollower(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)
	var follower int
	for id := range c.rafts {
		if id != leader {
			follower = id
			break
		}
	}

	if err := c.rafts[follower].ReadIndex(); err == nil {
		t.Fatal("ReadIndex on a follower must fail")
	}
}

// THE key test: a leader cut off from the majority must NOT be able to serve a
// linearizable read, even though it still believes it is the leader. Without
// the quorum-confirmation step, this would wrongly succeed and hand back
// state frozen at the moment of the partition.
func TestReadIndexFailsOnPartitionedLeader(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)

	// Cut the leader off from both followers. It has no way to learn it's been
	// deposed — it still reports itself as leader.
	c.disconnect(leader)
	time.Sleep(100 * time.Millisecond)

	if _, stillThinksItsLeader := c.rafts[leader].GetState(); !stillThinksItsLeader {
		t.Skip("leader stepped down on its own before we could test the partition case")
	}

	if err := c.rafts[leader].ReadIndex(); err == nil {
		t.Fatal("a partitioned leader must NOT be able to establish a read index — " +
			"it cannot know whether a new leader has since accepted writes")
	}
}

// After a write commits, a read index established afterward must guarantee the
// state machine has applied it.
func TestReadIndexReflectsCommittedWrites(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)
	for _, cmd := range []string{"a", "b", "c"} {
		if _, _, isLeader := c.rafts[leader].Propose(cmd); !isLeader {
			t.Fatalf("expected %d to still be leader", leader)
		}
	}
	if !c.waitApplied(3, 3*time.Second) {
		t.Fatal("commands should commit")
	}

	if err := c.rafts[leader].ReadIndex(); err != nil {
		t.Fatalf("ReadIndex failed: %v", err)
	}

	_, _, _, commitIdx, _, _ := c.rafts[leader].DebugState()
	c.rafts[leader].mu.Lock()
	applied := c.rafts[leader].lastApplied
	c.rafts[leader].mu.Unlock()
	if applied < commitIdx {
		t.Fatalf("after ReadIndex, lastApplied (%d) must have caught up to commitIndex (%d)",
			applied, commitIdx)
	}
}
