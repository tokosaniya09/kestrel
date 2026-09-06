package raft

import (
	"testing"
	"time"
)

type noopTransport struct{}

func (noopTransport) SendRequestVote(int, RequestVoteArgs) (RequestVoteReply, bool) {
	return RequestVoteReply{}, false
}
func (noopTransport) SendAppendEntries(int, AppendEntriesArgs) (AppendEntriesReply, bool) {
	return AppendEntriesReply{}, false
}
func (noopTransport) SendInstallSnapshot(int, InstallSnapshotArgs) (InstallSnapshotReply, bool) {
	return InstallSnapshotReply{}, false
}

func TestPersistAcrossRestart(t *testing.T) {
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

	for _, cmd := range []string{"a", "b", "c"} {
		if _, _, isLeader := c.rafts[leader].Propose(cmd); !isLeader {
			t.Fatalf("expected %d to still be leader", leader)
		}
	}
	if !c.waitApplied(3, 3*time.Second) {
		t.Fatal("commands should commit before restart")
	}

	beforeTerm, _, beforeLogLen, _, _, _ := c.rafts[follower].DebugState()

	c.restart(follower)

	afterTerm, afterRole, afterLogLen, _, _, _ := c.rafts[follower].DebugState()
	if afterRole != Follower {
		t.Fatalf("a restarted node must come back as a follower, got %s", afterRole)
	}
	if afterTerm != beforeTerm {
		t.Fatalf("term not persisted: had %d before restart, have %d after", beforeTerm, afterTerm)
	}
	if afterLogLen != beforeLogLen {
		t.Fatalf("log not persisted: had %d entries before restart, have %d after", beforeLogLen, afterLogLen)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, _, _, commit, _, _ := c.rafts[follower].DebugState()
		if commit >= 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, _, _, finalCommit, _, _ := c.rafts[follower].DebugState()
	if finalCommit < 3 {
		t.Fatalf("restarted node never relearned its commit index: %d", finalCommit)
	}
}

func TestNoDoubleVoteAcrossRestart(t *testing.T) {
	mp := NewMemoryPersister()
	r := NewRaft(0, []int{0, 1, 2}, noopTransport{}, mp)
	r.Start()

	reply := r.RequestVote(RequestVoteArgs{Term: 5, CandidateID: 1})
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted, got %+v", reply)
	}
	r.Stop()

	r2 := NewRaft(0, []int{0, 1, 2}, noopTransport{}, mp)
	r2.Start()
	defer r2.Stop()

	reply2 := r2.RequestVote(RequestVoteArgs{Term: 5, CandidateID: 2})
	if reply2.VoteGranted {
		t.Fatal("granted a second vote in a term already voted in before crashing — persistence isn't wired correctly")
	}
}

func TestClusterSurvivesLeaderRestart(t *testing.T) {
	c := makeCluster(3)
	defer c.stopAll()

	leader := c.checkOneLeader(t)
	for _, cmd := range []string{"x", "y"} {
		if _, _, isLeader := c.rafts[leader].Propose(cmd); !isLeader {
			t.Fatalf("expected %d to still be leader", leader)
		}
	}
	if !c.waitApplied(2, 3*time.Second) {
		t.Fatal("commands should commit before restart")
	}

	c.restart(leader)

	newLeader := c.checkOneLeader(t)
	if _, _, isLeader := c.rafts[newLeader].Propose("z"); !isLeader {
		t.Fatalf("expected %d to be leader", newLeader)
	}
	if !c.waitApplied(3, 3*time.Second) {
		t.Fatal("cluster should keep making progress after a leader restart")
	}
}
