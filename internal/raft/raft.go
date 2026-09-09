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

	// Snapshot state (Phase 7). snapshotIndex/snapshotTerm describe the last
	// entry summarized by snapshotData; log[0] always represents snapshotIndex
	// (see log.go). Before any snapshot, snapshotIndex is 0 — the same "nothing
	// compacted yet" state Phases 1-6 always had.
	snapshotIndex int
	snapshotTerm  int
	snapshotData  []byte

	// pendingSnapshot, when non-nil, is delivered via applyCh on applyLoop's
	// next pass — set when a snapshot is restored on startup or received via
	// InstallSnapshot, both cases where the local state machine needs the bytes
	// handed to it before normal operation continues.
	pendingSnapshot *ApplyMsg

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
		if term, votedFor, log, snapIndex, snapTerm, snapData, derr := decodeState(data); derr == nil {
			r.currentTerm = term
			r.votedFor = votedFor
			r.log = log
			r.snapshotIndex = snapIndex
			r.snapshotTerm = snapTerm
			r.snapshotData = snapData
			if snapIndex > 0 {
				// commitIndex/lastApplied are volatile per Figure 2 — but a
				// snapshot means the log entries below snapIndex no longer
				// EXIST for applyLoop to walk. Without this, applyLoop would
				// try to "apply" indices we can no longer read and panic. This
				// is the one place volatile state must be seeded from
				// persisted data, not left at zero.
				r.commitIndex = snapIndex
				r.lastApplied = snapIndex
				r.pendingSnapshot = &ApplyMsg{
					IsSnapshot:    true,
					SnapshotIndex: snapIndex,
					SnapshotTerm:  snapTerm,
					Snapshot:      snapData,
				}
			}
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

// Leader returns this node's best current guess at the cluster's leader, or -1
// if it doesn't know (e.g. an election is in progress). Phase 8 uses this for
// leader-redirection hints when a client asks the wrong node.
func (r *Raft) Leader() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.leaderID
}

// ApplyCh is where committed entries arrive, in increasing CommandIndex order.
// A later phase feeds these into the KV store's state machine.
func (r *Raft) ApplyCh() <-chan ApplyMsg { return r.applyCh }

// DebugState returns a snapshot of this node's role, term, log length, commit
// index, believed leader, and how far it's snapshotted. Diagnostic only — not
// used by the protocol itself, just by tests trying to see what's going on.
func (r *Raft) DebugState() (term int, role Role, logLen int, commitIndex int, leaderID int, snapshotIndex int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.role, len(r.log), r.commitIndex, r.leaderID, r.snapshotIndex
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
			r.broadcastReplication()
			time.Sleep(heartbeatInterval)
		} else {
			if elapsed >= timeout {
				r.startElection()
			}
			time.Sleep(tickInterval)
		}
	}
}

// applyLoop hands newly committed entries (and, first, any pending snapshot) to
// applyCh in order. It never sends while holding mu: it snapshots what's ready,
// unlocks, then sends.
func (r *Raft) applyLoop() {
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		r.mu.Lock()
		var pending *ApplyMsg
		if r.pendingSnapshot != nil {
			pending = r.pendingSnapshot
			r.pendingSnapshot = nil
		}
		var toApply []ApplyMsg
		for r.lastApplied < r.commitIndex {
			r.lastApplied++
			toApply = append(toApply, ApplyMsg{
				CommandIndex: r.lastApplied,
				Command:      r.log[r.logPos(r.lastApplied)].Command,
			})
		}
		r.mu.Unlock()

		if pending != nil {
			select {
			case r.applyCh <- *pending:
			case <-r.stopCh:
				return
			}
		}
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

func (r *Raft) InstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.handleInstallSnapshot(args)
}

// handleInstallSnapshot is the follower side of receiving a snapshot: a leader
// sends this when the entry a follower needs has already been compacted out of
// the leader's own log. Provided in full — this is the fiddliest single piece
// of Phase 7 (reconciling local state against an incoming snapshot), matching
// the project's pattern of providing genuinely hazardous, not-very-instructive
// slice surgery rather than having you reinvent it.
//
// Simplification: this ALWAYS discards the entire local log and replaces it
// with a fresh sentinel at the snapshot's boundary — even if some of the
// follower's existing entries were already consistent with the snapshot and
// could have been kept. The Raft paper (§7) describes that optimization;
// skipping it is simplest-correct, just not maximally efficient. A good
// stretch goal once everything else is solid.
func (r *Raft) handleInstallSnapshot(args InstallSnapshotArgs) InstallSnapshotReply {
	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}
	reply := InstallSnapshotReply{Term: r.currentTerm}
	if args.Term < r.currentTerm {
		return reply // stale leader
	}

	r.role = Follower
	r.leaderID = args.LeaderID
	r.resetElectionTimer()

	if args.LastIncludedIndex <= r.snapshotIndex {
		return reply // we already have this snapshot (or a newer one) — no-op
	}

	r.log = []LogEntry{{Term: args.LastIncludedTerm}}
	r.snapshotIndex = args.LastIncludedIndex
	r.snapshotTerm = args.LastIncludedTerm
	r.snapshotData = args.Data
	r.commitIndex = args.LastIncludedIndex
	r.lastApplied = args.LastIncludedIndex
	r.pendingSnapshot = &ApplyMsg{
		IsSnapshot:    true,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
		Snapshot:      args.Data,
	}
	r.persist()

	return reply
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

// broadcastReplication sends each peer either an InstallSnapshot (when the
// entry that peer needs next has already been compacted out of our log) or a
// normal AppendEntries. It handles the network fan-out and higher-term
// step-down for both, then hands a normal-term reply to
// handleInstallSnapshotReply or handleAppendEntriesReply for the bookkeeping.
func (r *Raft) broadcastReplication() {
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

			if r.needsSnapshot(peer) {
				args := r.buildInstallSnapshotArgs()
				r.mu.Unlock()

				reply, ok := r.transport.SendInstallSnapshot(peer, args)
				if !ok {
					return
				}
				r.mu.Lock()
				if reply.Term > r.currentTerm {
					r.becomeFollower(reply.Term)
					r.mu.Unlock()
					return
				}
				r.handleInstallSnapshotReply(peer, args, reply)
				r.mu.Unlock()
				return
			}

			args := r.buildAppendEntriesArgs(peer)
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
			r.handleAppendEntriesReply(peer, args, reply)
			r.mu.Unlock()
		}(peer)
	}
}

// confirmStillLeader sends a heartbeat round and reports whether a majority of
// the cluster acknowledged this node as leader at term. It is what turns "I
// believe I'm the leader" into "a majority just now confirmed I'm the leader."
//
// Why that distinction matters: a partitioned leader does NOT know it's been
// deposed — nothing informs it. It would keep answering reads from state
// frozen at the moment it was cut off. Requiring a fresh majority ack rules
// that out: any two majorities of the same cluster overlap in at least one
// node, and a node that has moved to a higher term rejects this heartbeat
// rather than acking it.
//
// It counts this node itself as one ack (a leader trivially agrees it's the
// leader), then adds peer acks. Steps down on any higher-term reply, exactly
// like the other fan-outs.
func (r *Raft) confirmStillLeader(term int) bool {
	r.mu.Lock()
	if r.role != Leader || r.currentTerm != term {
		r.mu.Unlock()
		return false
	}
	args := AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		PrevLogIndex: r.lastLogIndex(),
		PrevLogTerm:  r.lastLogTerm(),
		LeaderCommit: r.commitIndex,
	}
	peers := r.otherPeers()
	r.mu.Unlock()

	var (
		mu   sync.Mutex
		acks = 1 // ourselves
		wg   sync.WaitGroup
	)
	for _, peer := range peers {
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
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
			r.mu.Unlock()
			if reply.Success {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()
	// Re-check: we may have been deposed while unlocked (same discipline as
	// startElection's re-check before becomeLeader).
	if r.role != Leader || r.currentTerm != term {
		return false
	}
	return r.isMajority(acks)
}

// waitForApplied blocks until lastApplied reaches at least index, or timeout
// elapses. A simple poll, matching the tick-based style of applyLoop rather
// than introducing condition variables.
func (r *Raft) waitForApplied(index int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		applied := r.lastApplied
		r.mu.Unlock()
		if applied >= index {
			return true
		}
		time.Sleep(tickInterval)
	}
	return false
}
