package raft

import (
	"testing"
	"time"
)

// Snapshotting must actually shrink the log, without losing any committed data
// — lastLogIndex should be unchanged (the history is still known, just via the
// snapshot instead of individual entries).
func TestSnapshotDiscardsLog(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)
	for _, cmd := range []string{"a", "b", "c", "d", "e"} {
		if _, _, isLeader := c.rafts[leader].Propose(cmd); !isLeader {
			t.Fatalf("expected %d to still be leader", leader)
		}
	}
	if !c.waitApplied(5, 3*time.Second) {
		t.Fatal("commands should commit before snapshotting")
	}

	_, _, _, beforeCommit, _, _ := c.rafts[leader].DebugState()
	if beforeCommit < 5 {
		t.Fatalf("expected commitIndex >= 5, got %d", beforeCommit)
	}

	if err := c.rafts[leader].Snapshot(beforeCommit, []byte("fake-kv-state")); err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	_, _, afterLogLen, _, _, afterSnapIndex := c.rafts[leader].DebugState()
	if afterSnapIndex != beforeCommit {
		t.Fatalf("snapshotIndex = %d, want %d", afterSnapIndex, beforeCommit)
	}
	if afterLogLen != 1 {
		t.Fatalf("expected the log to shrink to just the sentinel (1), got %d entries", afterLogLen)
	}

	// The leader itself doesn't need an ApplyMsg for its own Snapshot() call —
	// it already had this data; that's exactly why it could summarize it. But
	// normal operation must still work afterward: propose one more entry.
	if _, _, isLeader := c.rafts[leader].Propose("f"); !isLeader {
		t.Fatalf("expected %d to still be leader after snapshotting", leader)
	}
	if !c.waitApplied(6, 3*time.Second) {
		t.Fatal("cluster should keep making progress after a snapshot")
	}
}

// A follower that falls far enough behind that the leader has already
// discarded the entries it needs must receive an InstallSnapshot instead —
// and its local state machine must actually be handed the snapshot bytes via
// ApplyCh, not just have Raft's internal bookkeeping updated silently.
func TestFollowerReceivesSnapshotAfterFallingBehind(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)
	var laggard int
	for id := range c.rafts {
		if id != leader {
			laggard = id
			break
		}
	}
	c.disconnect(laggard)

	for _, cmd := range []string{"a", "b", "c", "d", "e"} {
		if _, _, isLeader := c.rafts[leader].Propose(cmd); !isLeader {
			t.Fatalf("expected %d to still be leader", leader)
		}
	}
	if !c.waitApplied(5, 3*time.Second) {
		t.Fatal("commands should commit via leader + the one remaining follower")
	}

	// The leader compacts away everything the laggard would need for a normal
	// catch-up — forcing InstallSnapshot to be the only way forward for it.
	_, _, _, commitIdx, _, _ := c.rafts[leader].DebugState()
	if err := c.rafts[leader].Snapshot(commitIdx, []byte("state-through-e")); err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	c.reconnect(laggard)

	// Reconnecting an isolated node can trigger the same disruptive-rejoin
	// phenomenon found in Phase 5 (its election timer kept firing while cut
	// off, inflating its term) — which can force a NEW election, possibly
	// landing leadership on a DIFFERENT node than the one that just called
	// Snapshot(). Each node's snapshot state is independent, so if leadership
	// actually moved, the new leader has its own full, uncompacted log and
	// wouldn't otherwise know to send an InstallSnapshot at all — it would
	// just replicate the backlog via ordinary AppendEntries instead, which is
	// correct behavior but not what THIS test needs to exercise. Re-snapshot
	// on whichever node ends up leading (a no-op if it's still the same one)
	// so the test deterministically hits InstallSnapshot regardless of
	// incidental churn.
	newLeader := c.checkOneLeader(t)
	if err := c.rafts[newLeader].Snapshot(commitIdx, []byte("state-through-e")); err != nil {
		t.Fatalf("Snapshot failed on %d: %v", newLeader, err)
	}

	// The laggard's local "state machine" must actually receive the snapshot
	// via ApplyCh — checked through the cluster's recorded copy, since
	// drainApplied is the channel's only consumer (reading it a second way here
	// would race that goroutine for each message).
	want := []byte("state-through-e")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !c.hasAppliedSnapshot(laggard, want) {
		time.Sleep(50 * time.Millisecond)
	}
	if !c.hasAppliedSnapshot(laggard, want) {
		t.Fatal("laggard never received the snapshot via ApplyCh")
	}

	_, _, _, _, _, laggardSnapIndex := c.rafts[laggard].DebugState()
	if laggardSnapIndex != commitIdx {
		t.Fatalf("laggard's snapshotIndex = %d, want %d", laggardSnapIndex, commitIdx)
	}

	// Normal replication must continue working afterward. Use newLeader, not
	// the original leader variable — if churn happened, the original leader
	// may no longer BE the leader.
	if _, _, isLeader := c.rafts[newLeader].Propose("f"); !isLeader {
		t.Fatalf("expected %d to still be leader", newLeader)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, _, _, commit, _, _ := c.rafts[laggard].DebugState()
		if commit >= commitIdx+1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("laggard never resumed normal replication after receiving the snapshot")
}

// Snapshot metadata and data must survive a crash and restart, and a restarted
// node must initialize lastApplied/commitIndex from the snapshot — not from
// zero, which would make applyLoop try to re-walk log entries that no longer
// exist.
func TestSnapshotSurvivesRestart(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)
	for _, cmd := range []string{"a", "b", "c"} {
		if _, _, isLeader := c.rafts[leader].Propose(cmd); !isLeader {
			t.Fatalf("expected %d to still be leader", leader)
		}
	}
	if !c.waitApplied(3, 3*time.Second) {
		t.Fatal("commands should commit before snapshotting")
	}

	_, _, _, commitIdx, _, _ := c.rafts[leader].DebugState()
	if err := c.rafts[leader].Snapshot(commitIdx, []byte("snap-data")); err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	c.restart(leader)

	_, role, _, restartCommit, _, restartSnapIndex := c.rafts[leader].DebugState()
	if role != Follower {
		t.Fatalf("a restarted node must come back as a follower, got %s", role)
	}
	if restartSnapIndex != commitIdx {
		t.Fatalf("snapshotIndex not persisted: had %d, have %d after restart", commitIdx, restartSnapIndex)
	}
	if restartCommit < commitIdx {
		t.Fatalf("commitIndex must initialize from the snapshot on restart: got %d, want >= %d", restartCommit, commitIdx)
	}
}
