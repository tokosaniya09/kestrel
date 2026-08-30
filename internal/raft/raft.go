package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Role is a node's current role in the Raft protocol.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "?"
}

// Timing. Election timeouts are randomized in [min,max] so nodes don't all become
// candidates at once. Heartbeats must be frequent enough that a follower hears
// from the leader well within its election timeout.
const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
	tickInterval       = 10 * time.Millisecond
)

// Raft is one node. All mutable state is guarded by mu.
//
// The golden locking rule: NEVER send an RPC (call anything on r.transport) while
// holding mu. Snapshot what you need under the lock, unlock, then send. The RPC
// *handlers* (handleRequestVote / handleAppendEntries) run under the lock and
// must never send RPCs themselves. This is what keeps the cluster deadlock-free.
type Raft struct {
	mu        sync.Mutex
	id        int
	peers     []int // all node ids, including this one
	transport Transport

	// Core Raft state (persisted for real in Phase 6).
	currentTerm int
	votedFor    int // candidate id voted for in currentTerm, or -1
	role        Role
	leaderID    int // best guess at the current leader, or -1

	// Election timing.
	lastHeard       time.Time     // last time we heard from a leader or granted a vote
	electionTimeout time.Duration // current randomized timeout

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewRaft creates a node. peers must list every id in the cluster, including id.
func NewRaft(id int, peers []int, transport Transport) *Raft {
	r := &Raft{
		id:          id,
		peers:       peers,
		transport:   transport,
		currentTerm: 0,
		votedFor:    -1,
		role:        Follower,
		leaderID:    -1,
		stopCh:      make(chan struct{}),
	}
	r.resetElectionTimer()
	return r
}

// Start launches the node's background loop. Stop halts it (safe to call twice).
func (r *Raft) Start() { go r.run() }
func (r *Raft) Stop()  { r.stopOnce.Do(func() { close(r.stopCh) }) }

// GetState reports the current term and whether this node believes it is leader.
func (r *Raft) GetState() (term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role == Leader
}

// run is the heartbeat/election ticker. As leader it broadcasts heartbeats; as
// follower/candidate it starts an election once its timeout elapses.
func (r *Raft) run() {
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		r.mu.Lock()
		role := r.role
		elapsed := time.Since(r.lastHeard)
		timeout := r.electionTimeout
		r.mu.Unlock()

		if role == Leader {
			r.broadcastHeartbeats()
			time.Sleep(heartbeatInterval)
		} else {
			if elapsed >= timeout {
				r.startElection()
			}
			time.Sleep(tickInterval)
		}
	}
}

// --- RPC entry points: the transport calls these on the receiving node. Each
// locks and delegates to a handler you implement in election.go. ---

func (r *Raft) RequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.handleRequestVote(args)
}

func (r *Raft) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.handleAppendEntries(args)
}

// --- Provided helpers you'll call from election.go ---

// becomeFollower steps down to follower at the given term, clearing the vote.
// Call this whenever you observe a term higher than your own. Assumes mu held.
func (r *Raft) becomeFollower(term int) {
	r.currentTerm = term
	r.role = Follower
	r.votedFor = -1
	r.resetElectionTimer()
}

// resetElectionTimer marks "just heard from the cluster" and rolls a fresh
// randomized timeout. Assumes mu held.
func (r *Raft) resetElectionTimer() {
	r.lastHeard = time.Now()
	r.electionTimeout = randomElectionTimeout()
}

func randomElectionTimeout() time.Duration {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(span))
}

func (r *Raft) otherPeers() []int {
	out := make([]int, 0, len(r.peers)-1)
	for _, p := range r.peers {
		if p != r.id {
			out = append(out, p)
		}
	}
	return out
}

// isMajority reports whether votes is a strict majority of the cluster. Assumes
// mu held (reads len(r.peers), which never changes in this phase).
func (r *Raft) isMajority(votes int) bool {
	return votes*2 > len(r.peers)
}

// requestVotesFromPeers sends RequestVote to every peer concurrently and returns
// how many GRANTED (not counting this node's own vote). If any reply carries a
// higher term, it steps this node down to follower. Provided so you don't have to
// write the goroutine fan-out.
func (r *Raft) requestVotesFromPeers(args RequestVoteArgs) int {
	var (
		mu      sync.Mutex
		granted int
		wg      sync.WaitGroup
	)
	for _, peer := range r.otherPeers() {
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			reply, ok := r.transport.SendRequestVote(peer, args)
			if !ok {
				return
			}
			r.mu.Lock()
			if reply.Term > r.currentTerm {
				r.becomeFollower(reply.Term)
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()
			if reply.VoteGranted {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()
	return granted
}

// broadcastHeartbeats sends an empty AppendEntries to every peer. If a reply has a
// higher term, this leader steps down. Provided.
func (r *Raft) broadcastHeartbeats() {
	r.mu.Lock()
	if r.role != Leader {
		r.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{Term: r.currentTerm, LeaderID: r.id}
	r.mu.Unlock()

	for _, peer := range r.otherPeers() {
		go func(peer int) {
			reply, ok := r.transport.SendAppendEntries(peer, args)
			if !ok {
				return
			}
			r.mu.Lock()
			if reply.Term > r.currentTerm {
				r.becomeFollower(reply.Term)
			}
			r.mu.Unlock()
		}(peer)
	}
}
