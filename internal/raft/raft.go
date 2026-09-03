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

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
	tickInterval       = 10 * time.Millisecond
)

// Raft is one node. All mutable state is guarded by mu.
//
// The golden locking rule: NEVER send an RPC while holding mu. Snapshot what you
// need under the lock, unlock, then send. RPC *handlers* and the leader-side
// decision functions you write in replication.go run under the lock and must
// never send RPCs themselves.
type Raft struct {
	mu        sync.Mutex
	id        int
	peers     []int
	transport Transport

	// Election state (Phase 4).
	currentTerm int
	votedFor    int
	role        Role
	leaderID    int

	// Replicated log (Phase 5). 1-indexed; log[0] is a Term-0 sentinel.
	log         []LogEntry
	commitIndex int // highest index known committed
	lastApplied int // highest index handed to the state machine so far

	// Leader-only bookkeeping (Phase 5), (re)initialized in becomeLeader.
	nextIndex  map[int]int // next log index to send each peer
	matchIndex map[int]int // highest index known replicated on each peer (+ self)

	applyCh chan ApplyMsg

	persister Persister // Phase 6: durably saves currentTerm/votedFor/log

	lastHeard       time.Time
	electionTimeout time.Duration

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewRaft creates a node. peers must list every id in the cluster, including
// id. persister recovers currentTerm/votedFor/log if this node has run before
// (a restart after a real or simulated crash); role, leaderID, commitIndex, and
// lastApplied are volatile and always start fresh, per the Raft paper's split
// between persistent and volatile state (Figure 2) — a restarted node always
// comes back as a plain follower, never resumes being leader.
func NewRaft(id int, peers []int, transport Transport, persister Persister) *Raft {
	r := &Raft{
		id:          id,
		peers:       peers,
		transport:   transport,
		currentTerm: 0,
		votedFor:    -1,
		role:        Follower,
		leaderID:    -1,
		log:         []LogEntry{{Term: 0}}, // sentinel at index 0
		commitIndex: 0,
		lastApplied: 0,
		applyCh:     make(chan ApplyMsg, 256),
		persister:   persister,
		stopCh:      make(chan struct{}),
	}
	if data, err := persister.Load(); err == nil && len(data) > 0 {
		if term, votedFor, log, derr := decodeState(data); derr == nil {
			r.currentTerm = term
			r.votedFor = votedFor
			r.log = log
		}
		// A decode error on existing data means corrupted persisted state — a
		// real system should fail loudly rather than silently start fresh
		// (silently discarding it could enable exactly the double-vote /
		// lost-log-entry bugs persistence exists to prevent). Treating it as
		// "fresh" here is a known simplification — a good stretch goal.
	}
	r.resetElectionTimer()
	return r
}

// Start launches the node's background loops. Stop halts them (safe to call twice).
func (r *Raft) Start() {
	go r.run()
	go r.applyLoop()
}
func (r *Raft) Stop() { r.stopOnce.Do(func() { close(r.stopCh) }) }

// GetState reports the current term and whether this node believes it is leader.
func (r *Raft) GetState() (term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role == Leader
}

// ApplyCh is where committed entries arrive, in increasing CommandIndex order.
// A later phase feeds these into the KV store's state machine.
func (r *Raft) ApplyCh() <-chan ApplyMsg { return r.applyCh }

// DebugState returns a snapshot of this node's role, term, log length, commit
// index, and believed leader. Diagnostic only — not used by the protocol
// itself, just by tests trying to see what's actually going on.
func (r *Raft) DebugState() (term int, role Role, logLen int, commitIndex int, leaderID int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role, len(r.log), r.commitIndex, r.leaderID
}

// Propose appends command to the leader's own log for replication. Returns the
// index the command occupies, the current term, and whether this node is
// actually the leader (if false, the command was NOT accepted — the caller must
// find the real leader, which a later phase automates).
func (r *Raft) Propose(command interface{}) (index int, term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != Leader {
		return -1, r.currentTerm, false
	}
	r.log = append(r.log, LogEntry{Term: r.currentTerm, Command: command})
	r.persist() // the log just grew — must survive a crash before we tell the
	// caller it was accepted
	index = r.lastLogIndex()
	r.matchIndex[r.id] = index // the leader always "has" what it just appended
	return index, r.currentTerm, true
}

// run is the heartbeat/election ticker.
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
			r.broadcastAppendEntries()
			time.Sleep(heartbeatInterval)
		} else {
			if elapsed >= timeout {
				r.startElection()
			}
			time.Sleep(tickInterval)
		}
	}
}

// applyLoop hands newly committed entries to applyCh in order. It never sends
// while holding mu: it snapshots what's newly committed, unlocks, then sends.
func (r *Raft) applyLoop() {
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		r.mu.Lock()
		var toApply []ApplyMsg
		for r.lastApplied < r.commitIndex {
			r.lastApplied++
			toApply = append(toApply, ApplyMsg{
				CommandIndex: r.lastApplied,
				Command:      r.log[r.lastApplied].Command,
			})
		}
		r.mu.Unlock()

		for _, m := range toApply {
			select {
			case r.applyCh <- m:
			case <-r.stopCh:
				return
			}
		}
		time.Sleep(tickInterval)
	}
}

// --- RPC entry points ---

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

// --- Provided helpers ---

func (r *Raft) becomeFollower(term int) {
	r.currentTerm = term
	r.role = Follower
	r.votedFor = -1
	r.persist() // currentTerm and votedFor just changed — must survive a crash
	r.resetElectionTimer()
}

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

func (r *Raft) isMajority(votes int) bool {
	return votes*2 > len(r.peers)
}

// requestVotesFromPeers sends RequestVote to every peer concurrently and returns
// how many GRANTED (not counting this node's own vote). Steps down on any
// higher-term reply.
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

// broadcastAppendEntries sends AppendEntries to every peer, built per-peer via
// your buildAppendEntriesArgs (replication.go). Handles the network fan-out and
// higher-term step-down; hands a normal-term reply to your
// handleAppendEntriesReply for the actual replication/commit decisions.
func (r *Raft) broadcastAppendEntries() {
	r.mu.Lock()
	if r.role != Leader {
		r.mu.Unlock()
		return
	}
	peers := r.otherPeers()
	r.mu.Unlock()

	for _, peer := range peers {
		go func(peer int) {
			r.mu.Lock()
			if r.role != Leader {
				r.mu.Unlock()
				return
			}
			args := r.buildAppendEntriesArgs(peer) // YOU implement (replication.go)
			r.mu.Unlock()

			reply, ok := r.transport.SendAppendEntries(peer, args)
			if !ok {
				return
			}

			r.mu.Lock()
			if reply.Term > r.currentTerm {
				r.becomeFollower(reply.Term)
				r.mu.Unlock()
				return
			}
			r.handleAppendEntriesReply(peer, args, reply) // YOU implement
			r.mu.Unlock()
		}(peer)
	}
}
