# Kestrel — Design

Kestrel is a distributed, replicated, crash-safe key-value database written from
scratch in Go, using only the standard library. This document covers the
architecture, the data formats, and the reasoning behind the significant
decisions.

## Contents

1. [Overview](#1-overview)
2. [Storage engine](#2-storage-engine)
3. [Consensus](#3-consensus)
4. [Distributed layer](#4-distributed-layer)
5. [Design decisions](#5-design-decisions)
6. [Testing](#6-testing)
7. [Limitations](#7-limitations)

---

## 1. Overview

The system is three layers, each built on the one below it:

```
Layer 3   Distributed database    client RPC, leader redirection, linearizable reads
              |
Layer 2   Raft consensus          election, log replication, persistence, snapshots
              |
Layer 1   Storage engine          durable single-node LSM-tree key-value storage
```

Every node runs all three layers. One node is elected leader; the rest are
followers. Writes go through the leader, which replicates them via Raft before
considering them committed. Each node then applies committed operations to its
own private storage engine.

The core idea is **state machine replication**: agree on an ordered log of
operations, then have every node replay that same log into the same state
machine. Because each node applies an identical sequence of operations in an
identical order, the storage engines converge to identical state. There is no
shared storage — a three-node cluster holds three complete, independent copies
of the database.

```
                    +-------- Client --------+
                    |  Put / Get / Delete    |
                    +-----------+------------+
                                |  contacts any node; follows
                                |  a redirect to the leader
                                v
     +--------------------------------------------------+
     |                  NODE A (leader)                  |
     |   RPC server  -->  Raft  -->  storage engine      |
     +--------------------+-----------------------------+
                          | AppendEntries
             +------------+------------+
             v                         v
   +------------------+      +------------------+
   | NODE B (follower)|      | NODE C (follower)|
   | Raft --> storage |      | Raft --> storage |
   +------------------+      +------------------+
```

---

## 2. Storage engine

**Package:** `internal/storage`

A log-structured merge-tree providing durable `Put`, `Get`, and `Delete` on a
single machine.

```
   write -->  +---------+  appended and fsynced before anything else
              |   WAL   |
              +----+----+
                   v
              +----------+  sorted, in memory (skiplist)
              | memtable |
              +----+-----+
                   |  flushed when it exceeds the size threshold
                   v
     +------------------------------------+  immutable sorted files
     |  SSTable   SSTable   SSTable  ...  |
     +------------------------------------+
                   |  compaction merges them, dropping dead versions
                   v
```

### Write-ahead log

Every mutation is appended to the WAL and fsynced before it is applied to the
memtable. On startup the WAL is replayed to rebuild the memtable, so a crash
loses nothing that was acknowledged. Records use a length-prefixed binary
format, big-endian:

```
[kind:1][keyLen:4][key][valueLen:4][value]
```

`kind` distinguishes a write from a deletion. The same encoding is reused for
SSTable data entries, so one codec defines what a record looks like on disk.

### Memtable

An in-memory skiplist, kept sorted by key. Sorted order is required because
flushing produces a sorted SSTable, and a hash map has no order to flush.

Deletions are stored as **tombstones** — a record with the delete kind — rather
than by removing the entry. A tombstone must remain visible to shadow older
values that still exist in SSTables on disk.

### SSTables

When the memtable exceeds its threshold it is written out as an immutable
sorted file. The file layout, big-endian throughout:

```
+----------------------------------------------------+  offset 0
|  DATA    records in ascending key order             |
+----------------------------------------------------+
|  INDEX   [keyLen:4][key][offset:8] per key          |
+----------------------------------------------------+
|  FILTER  [k:4][m:4][bits...]  (Bloom filter)        |
+----------------------------------------------------+
|  FOOTER  [indexOff:8][indexLen:8]                   |  fixed 36 bytes
|          [filterOff:8][filterLen:8][magic:4]        |
+----------------------------------------------------+  end of file
```

A reader works backwards: the footer is a fixed size, so it can always be found
at `fileSize - 36`. It points at the index, and the index points at individual
records. This ordering exists because a writer streams forward and cannot know
the index's offset or length until it has finished writing the data section —
so that information has to go at the end. The same shape appears in Parquet and
in RocksDB's SST files.

Files are published with a temp-file write followed by an atomic rename, so a
crash can never leave a partially written `.sst` for a reader to find.

### Reads

A lookup checks the memtable first, then SSTables newest to oldest. The first
source that *contains* the key wins — including when that entry is a tombstone,
in which case the key is reported absent and older sources are not consulted.
Collapsing "deleted here" and "not present here" into a single condition would
let deleted keys reappear from older files.

### Bloom filters

Each SSTable carries a Bloom filter over its keys: a bit array plus `k` hash
positions per key. Testing a key returns "definitely absent" or "possibly
present", never a false negative. A lookup consults the filter before searching
the index, so files that provably lack the key are skipped entirely.

The implementation uses 10 bits per key with 7 hash positions, giving a false
positive rate under 1%. The `k` positions are derived from a single FNV-1a hash
split into two halves, combined as `h1 + i*h2` — the Kirsch-Mitzenmacher double
hashing technique, which behaves like `k` independent hashes at the cost of one.

### Compaction

`Compact` merges all current SSTables into one via a k-way merge of their
sorted contents, keeping only the newest version of each key. Old files are
deleted afterward.

Because this is a **full** compaction, tombstones can be discarded during the
merge: every older value a tombstone was shadowing is part of the same merge and
is dropped alongside it, leaving nothing for the tombstone to shadow. This is
specific to merging everything at once. A partial compaction — the leveled kind
production engines use — must retain tombstones until the bottom level, since an
untouched file may still hold an older value that would otherwise resurface.

---

## 3. Consensus

**Package:** `internal/raft`

An implementation of Raft, following the paper's Figure 2.

### Roles and terms

Each node is a **follower**, **candidate**, or **leader**. Time is divided into
**terms**, numbered periods with at most one leader each, acting as a logical
clock. Every RPC carries the sender's term, and the governing rule is that a
node observing a term higher than its own immediately adopts it and reverts to
follower. This is what makes stale leaders and stale candidates stand down
without any explicit coordination.

### Election

A follower that hears nothing from a leader before its **randomized election
timeout** (150-300 ms) becomes a candidate: it increments the term, votes for
itself, and requests votes from its peers. A majority makes it leader, after
which heartbeats suppress further elections. Randomizing the timeout prevents
every node from becoming a candidate simultaneously and splitting the vote
indefinitely.

Two properties make one-leader-per-term hold. A node grants at most one vote per
term, and any two majorities of the same cluster share at least one node — so
two candidates cannot both collect a majority in the same term.

A candidate is also refused a vote unless its log is at least as up to date as
the voter's, compared by last log term and then last log index. Without this, a
node that had been partitioned and fallen behind could win an election and
overwrite committed history with its stale log.

### Log replication

The leader appends a client command to its own log, then replicates it with
`AppendEntries`. Each message carries the index and term of the entry that
should immediately precede the new ones; a follower rejects the message unless
its own log matches at that point. On rejection the leader decrements its
`nextIndex` for that follower and retries, walking backwards until the two logs
agree, then overwriting anything after the divergence. There is no separate
catch-up path — repair is the ordinary replication path, retried from an earlier
position.

Per-follower state is tracked as `nextIndex` (what to send next) and
`matchIndex` (what is known replicated).

An entry is **committed** once a majority holds it, after which it is applied to
the state machine. Critically, a leader advances its commit index only for
entries from its **own current term**. Counting replicas of an older-term entry
is not sufficient to declare it committed — the paper's section 5.4.2 — because
a specific sequence of leader changes can otherwise overwrite an entry that was
already reported committed. Once one of the leader's own entries reaches a
majority, everything preceding it is committed transitively.

### Persistence

`currentTerm`, `votedFor`, and the log are written to disk before the node
replies to any RPC that changed them. Without this, a restarted node could grant
a second vote in a term it had already voted in, or lose an entry it had already
acknowledged.

Role, commit index, and applied index are deliberately *not* persisted — a
restarted node always rejoins as a follower and rediscovers the rest. The one
exception is a node restoring a snapshot, which must seed its applied index from
the snapshot, since the log entries below it no longer exist to be replayed.

Serialization uses `encoding/gob`, because log entries hold an `interface{}`
command whose concrete type varies. `gob.Register` must know that type, and its
registry is process-global.

### Snapshots

The log cannot grow forever. `Snapshot(index, data)` records that the
application has durably captured everything through a committed index, after
which the log below it is discarded.

Once a prefix is discarded, slice position no longer equals log index. All log
access goes through a translation helper, so the rest of the implementation is
unaffected by the offset.

When a follower falls far enough behind that the entry it needs has already been
compacted away, the leader cannot send it via `AppendEntries` — it no longer has
that entry either. It sends `InstallSnapshot` instead. The leader decides per
peer which of the two to use, based on how far behind that specific peer is.

---

## 4. Distributed layer

**Packages:** `internal/node`, `internal/rpc`, `internal/client`

### Node

A `Node` pairs a Raft instance with a private storage engine. Its apply loop
drains committed entries from Raft and dispatches each to the storage engine —
which is what makes the storage engine the replicated state machine, without the
storage engine knowing Raft exists.

Commands carry an **ID**. `Propose` returns a log index, but not a guarantee: if
leadership changes before that index commits, a different client's command can
end up occupying it. A caller that checked only whether the index committed
would report success for a write that never happened. Matching the applied
command's ID against the proposed one closes that gap.

### RPC

Transport is Go's `net/rpc` over TCP, which uses gob as its wire format. Each
node serves two registered services on a single listener: `RaftService` for
inter-node consensus traffic, and `KVService` for external clients.

Raft's `Transport` is an interface, so the in-memory implementation used by
tests and the real TCP implementation are interchangeable, and the consensus
code is identical under both.

A redirect travels in the **reply payload**, not as a returned error, because
`net/rpc` serializes errors as plain strings — a typed error and its fields
would not survive the trip. Expected, actionable outcomes belong in the response
body; genuinely exceptional ones belong in the transport error.

### Client

The client knows every node's address but not which is the leader. It tries one,
follows the redirect if that node isn't the leader, and falls back to
round-robin when a node is unreachable. A successful node is cached as the next
attempt's first guess.

### Linearizable reads

A plain `Get` is answered by any node from its local state. It is fast and
requires no consensus, but it can be stale — the node may not yet have applied a
write that already committed elsewhere.

`LinearizableGet` uses the **ReadIndex** protocol. Before answering, the leader
records its current commit index, confirms via a heartbeat round that a majority
still recognizes it as leader in its current term, waits for its state machine
to apply through the recorded index, and only then reads.

The quorum confirmation is what makes this correct. A partitioned leader has no
way to learn it has been deposed — nothing tells it — so routing reads through
"the leader" is not by itself sufficient; it would serve state frozen at the
moment of the partition. A fresh majority acknowledgement rules that out, since
any two majorities overlap and a node at a higher term rejects the heartbeat
rather than acknowledging it.

Notably this requires no log write. A heartbeat round is enough to establish the
guarantee.

---

## 5. Design decisions

**LSM-tree rather than B+tree.** Append-only writes pair naturally with Raft's
append-only log, and the LSM structure decomposes into several cooperating
pieces with clear responsibilities. A B+tree would favor read-heavy workloads
and in-place updates instead.

**Raft implemented from scratch.** Using a consensus library would remove the
substance of the project.

**Go.** Goroutines and channels map cleanly onto Raft's timers and concurrent
RPCs, the standard library covers networking and serialization without external
dependencies, and garbage collection keeps attention on the algorithm.

**Full compaction rather than leveled.** Simplest correct behavior, and it makes
tombstone handling straightforward. Leveled compaction is the standard upgrade.

**ReadIndex rather than leader leases or log reads.** Routing reads through the
log is correct but costs a full consensus round. Leases are faster but depend on
clock assumptions. ReadIndex needs only a heartbeat round and no timing
assumptions.

**Static membership.** Dynamic membership is genuinely subtle and is best added
once the core is stable.

**A single mutex per Raft node, never held across an RPC.** State changes happen
under the lock; anything that sends a message snapshots what it needs, releases,
sends, then re-acquires and re-validates. Holding a lock across a network call
deadlocks readily — two candidates each awaiting the other's reply while each
holds the lock the other's handler needs — and a slow peer would otherwise stall
the entire node.

---

## 6. Testing

**Storage engine:** unit tests covering the read path across memtable and
SSTables, tombstone shadowing, SSTable round-trips, compaction correctness
(including tombstone removal), Bloom filter false-negative freedom, and
durability across restart.

**Consensus:** an in-memory network that can disconnect, crash, and restart
nodes, used to test election, re-election after leader failure, refusal to elect
without a majority, replication and cross-node agreement, follower catch-up
after a partition heals, persistence across restart, snapshot transfer, and
ReadIndex behavior — including a partitioned leader correctly refusing to serve
a linearizable read.

**Distributed layer:** replication over real TCP sockets, leader redirection
from a deliberately wrong first guess, and client recovery across a leader
failover.

Election and replication timing is nondeterministic, so the suite is intended to
be run repeatedly:

```bash
go test -count=10 ./...
```

Two real bugs in this implementation were found only by repeated runs rather
than single passes. One was a missing log-freshness check in the vote handler,
which allowed a node that had been partitioned and fallen behind to win an
election with an empty log and strand already-committed entries on the rest of
the cluster.

---

## 7. Limitations

- **Reads default to eventual consistency.** `Get` may lag the leader.
  `LinearizableGet` provides the strong guarantee at the cost of a heartbeat
  round-trip.
- **Static membership.** Nodes cannot join or leave a running cluster.
- **Received snapshots are not installed into the state machine.** Raft's
  bookkeeping is updated, but a node recovers its data by replaying its own
  persisted log rather than from the snapshot's contents.
- **Compaction has a crash window.** A crash between writing the merged SSTable
  and deleting the originals leaks the old files. Reads remain correct, since
  the newer file wins. A manifest recording the live SSTable set would close it.
- **SSTable indexes are held fully in memory.** A sparse index — offsets for
  every Nth key, with a short scan within a block — is the standard reduction.
- **Compaction is synchronous** and runs under the database lock.
- **Each RPC opens a fresh connection.** Production systems pool persistent
  connections per peer.
- **No TLS or authentication.** Plaintext TCP.