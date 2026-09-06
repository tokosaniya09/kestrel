package raft
import "fmt"

// This is your Phase 7 implementation file: four functions. Three run under
// r.mu (their callers hold the lock — same discipline as replication.go); one,
// Snapshot(), is the public entry point an application (the eventual KV store)
// calls, and takes the lock itself.
//
// The fiddly index-translation plumbing (logPos, and the follower-side
// handleInstallSnapshot) is provided. What's yours is the genuinely
// Raft-specific decision-making: when a node should summarize its own log, when
// a LEADER realizes a given follower needs a snapshot instead of a normal
// AppendEntries, and how to update leader bookkeeping once one's been sent.

// Snapshot tells this Raft node that the application has durably captured
// everything through index (as data) and it's safe to discard log entries up to
// and including it. Any node can call this — leader or follower — since each
// node's own state machine decides independently when it's snapshotted enough
// to be worth compacting.
//
func (r *Raft) Snapshot(index int, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index <= r.snapshotIndex {
		return nil // already snapshotted at least this far — nothing to do
	}
	if index > r.commitIndex {
		return fmt.Errorf("cannot snapshot index %d: not yet committed (commitIndex=%d)", index, r.commitIndex)
	}

	term := r.termAt(index) // capture BEFORE truncating — this becomes the new sentinel's term
	r.log = append([]LogEntry{{Term: term}}, r.log[r.logPos(index)+1:]...)
	r.snapshotIndex = index
	r.snapshotTerm = term
	r.snapshotData = data
	r.persist()
	return nil
}

// needsSnapshot reports whether peer's next expected entry (nextIndex[peer])
// has already been compacted out of our own log — meaning a normal
// AppendEntries is impossible (we no longer HAVE that entry to send) and an
// InstallSnapshot is required instead.
//
func (r *Raft) needsSnapshot(peer int) bool {
	return r.nextIndex[peer] <= r.snapshotIndex
}

// buildInstallSnapshotArgs assembles the RPC to send: our current snapshot,
// whole. (Real systems chunk this; we send it in one shot — see rpc.go.)
//
func (r *Raft) buildInstallSnapshotArgs() InstallSnapshotArgs {
	return InstallSnapshotArgs{
		Term:              r.currentTerm,
		LeaderID:          r.id,
		LastIncludedIndex: r.snapshotIndex,
		LastIncludedTerm:  r.snapshotTerm,
		Data:              r.snapshotData,
	}
}

// handleInstallSnapshotReply processes a follower's response to an
// InstallSnapshot you sent. Unlike AppendEntries, there's no
// success/failure branch to handle — a follower that receives a
// well-formed snapshot at a valid term always accepts it, so a reply reaching
// here (past the higher-term check broadcastReplication already did for you)
// means it worked: update your bookkeeping to reflect that peer now has
// everything through args.LastIncludedIndex.
//
func (r *Raft) handleInstallSnapshotReply(peer int, args InstallSnapshotArgs, reply InstallSnapshotReply) {
	if r.role != Leader || args.Term != r.currentTerm {
		return // stale reply for a round we've since moved past
	}
	if args.LastIncludedIndex > r.matchIndex[peer] {
		r.matchIndex[peer] = args.LastIncludedIndex
	}
	r.nextIndex[peer] = args.LastIncludedIndex + 1
}