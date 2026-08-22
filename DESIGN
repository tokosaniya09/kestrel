# Kestrel — Design Doc

> A distributed, replicated, crash-safe key–value database, built from scratch.
> Working codename: **Kestrel**. Rename it to whatever you like.

**Status:** Draft v0.1
**Author:** you
**One-line pitch:** "I built the database *and* the replication layer underneath it — the stuff you normally `docker run`."

---

## 0. How to read this doc

You said you don't know any of this yet, so this doc is written to be *learned from*, not just followed. Every term that matters is defined the first time it appears, and there's a full glossary at the end (Section 11). When you see a **[decision]** tag, that's a fork in the road where I picked a default and explained why — those are the ones worth arguing with me about.

Read it top to bottom once for the shape of the thing. Don't try to understand every detail on the first pass. You'll build understanding phase by phase.

---

## 1. What we're building (TL;DR)

We're building a **key–value store** — think a giant persistent dictionary. You can:

- `PUT(key, value)` — store a value
- `GET(key)` — read it back
- `DELETE(key)` — remove it
- `SCAN(start, end)` — read a range of keys in sorted order

That alone is *Layer 1*. Anyone can build that in an afternoon with a hash map. What makes this a real engineering project is that we're going to make it:

1. **Durable** — if the process crashes mid-write, no committed data is lost. (Layer 1)
2. **Replicated** — the data lives on multiple machines, and they agree on it. (Layer 2)
3. **Fault-tolerant** — the cluster keeps working correctly even when some machines crash or the network splits. (Layer 2)
4. **Linearizable** — from the outside, the whole distributed cluster behaves as if it were one single, correct machine. This is the hard, prestigious guarantee. (Layer 3)

So the three layers, which are also the three chapters of the build:

```
Layer 3:  Distributed KV Database   ← client-facing server, ties it all together
              │
Layer 2:  Raft consensus            ← makes N machines agree, survives failures
              │
Layer 1:  Storage engine (LSM-tree) ← durable single-node key-value storage
```

Each layer sits on the one below it. We build bottom-up.

---

## 2. Goals and non-goals

**Goals**
- Learn how a database actually stores bytes durably on disk (not "I called Postgres").
- Learn how distributed systems reach agreement despite crashes and network failures.
- Be able to survive a 40-minute technical interview being grilled on any part of it.
- Produce benchmarks and a linearizability test that *prove* it works.

**Non-goals (things we deliberately skip, at least at first)**
- SQL, a query planner, joins. This is key–value, not a relational DB.
- Transactions across multiple keys (stretch goal only).
- Sharding / horizontal scaling of data across many Raft groups (stretch goal).
- Authentication, TLS, a fancy admin UI. Not the point.
- Beating RocksDB on performance. We want *correct and understood*, not *fastest*.

Naming non-goals explicitly is itself an interview signal — it shows you scope deliberately.

---

## 3. The big picture

Here's the whole system with 3 nodes (the smallest cluster that tolerates 1 failure):

```
                         ┌─────────── Client ───────────┐
                         │  PUT(k,v) / GET(k) / DELETE   │
                         └───────────────┬───────────────┘
                                         │ (client talks to any node;
                                         │  gets redirected to the leader)
                                         ▼
        ┌────────────────────────────────────────────────────────┐
        │                        NODE A  (LEADER)                 │
        │  ┌──────────────┐   ┌──────────────┐   ┌─────────────┐  │
        │  │  RPC Server  │──▶│ Raft module  │──▶│  Storage    │  │
        │  │ (Layer 3)    │   │ (Layer 2)    │   │  engine     │  │
        │  └──────────────┘   └──────┬───────┘   │ (Layer 1)   │  │
        │                            │           └─────────────┘  │
        └────────────────────────────┼───────────────────────────┘
                    AppendEntries RPC │ (replicate the log)
                       ┌──────────────┴──────────────┐
                       ▼                              ▼
        ┌───────────────────────┐      ┌───────────────────────┐
        │   NODE B (FOLLOWER)   │      │   NODE C (FOLLOWER)   │
        │  Raft ─▶ Storage      │      │  Raft ─▶ Storage      │
        └───────────────────────┘      └───────────────────────┘
```

The key idea: **every node runs all three layers**. One node is elected **leader**; the others are **followers**. All writes go through the leader, which uses Raft to replicate them to the followers before considering them done. Each node independently applies the agreed-upon writes to its own local storage engine. The storage engines end up identical because they apply the exact same sequence of operations in the exact same order.

That last sentence is the whole trick of the system. Say it back to yourself: **agree on an ordered log of operations, then everyone replays the same log into the same state machine.** That's called *state machine replication*, and it's the heart of Raft.

---

## 4. Layer 1 — The storage engine (LSM-tree)

**Job:** durably store and retrieve sorted key–value pairs on a single machine.

**[decision] LSM-tree, not B+tree.**
There are two classic ways to build a storage engine:
- **B+tree** — what Postgres/MySQL use. Great for reads, updates data in place.
- **LSM-tree** (Log-Structured Merge tree) — what RocksDB/LevelDB/Cassandra use. Writes are append-only and fast; reads are a bit more work.

We're building an **LSM-tree** because (a) it's more fun and more educational — you build several cooperating pieces, and (b) append-only writes pair naturally with Raft's append-only log, so the mental models reinforce each other. A B+tree is a fine alternative if you want the other flavor; say so and I'll re-spec.

### 4.1 The pieces of an LSM-tree

```
   write ──▶  ┌─────────┐   append first, for durability
              │   WAL   │   (write-ahead log, on disk)
              └────┬────┘
                   ▼
              ┌─────────┐   sorted, in memory
              │ Memtable│   (a skiplist)
              └────┬────┘
                   │ when full, freeze it and flush to disk
                   ▼
     ┌──────────────────────────────────┐   immutable sorted files on disk
     │ SSTable  SSTable  SSTable  ...    │   (Sorted String Tables)
     └──────────────────────────────────┘
                   ▲
                   │ background job merges/cleans these
              ┌─────────┐
              │Compaction│
              └─────────┘
```

- **WAL (Write-Ahead Log):** Before we touch anything in memory, we append the write to a log file on disk and `fsync` it (force it to the physical disk). This is the durability guarantee: if we crash right after, we replay the WAL on restart and no committed write is lost. *Write to the log first, always* — that's the "write-ahead" rule.

- **Memtable:** An in-memory sorted structure (we'll use a **skiplist** — a simple, elegant sorted structure that's easier to implement than a balanced tree). New writes go here after the WAL. Reads check here first because it has the freshest data.

- **SSTable (Sorted String Table):** When the memtable gets big (say 4 MB), we freeze it and write it out to disk as an immutable, sorted file. Immutable = never modified after creation, which makes everything simpler (no locks, easy caching, safe to read while compacting). Each SSTable also carries:
  - a **sparse index** (key → file offset, for every Nth key) so we can seek instead of scanning, and
  - a **Bloom filter** — a tiny probabilistic structure that answers "is this key *definitely not* here?" instantly, so reads can skip SSTables that don't contain the key without touching disk.

- **Compaction:** Over time you accumulate many SSTables with overlapping/overwritten keys and deleted keys. A background job merges them, keeps only the newest value per key, and drops deleted keys. This reclaims space and keeps reads fast. **[decision]** We'll start with the simplest scheme (**size-tiered**: merge similarly-sized files) and optionally graduate to **leveled compaction** (LevelDB-style) later. Leveled is what you'd talk about in an interview, but tiered is easier to get correct first.

### 4.2 How reads, writes, and deletes work

- **PUT:** append to WAL → insert into memtable. Done. (Fast — no disk seeks.)
- **DELETE:** we can't erase from an immutable SSTable, so we write a **tombstone** — a marker meaning "this key is deleted." It shadows older values until compaction physically removes both.
- **GET:** check memtable → frozen memtables → SSTables newest-to-oldest (using Bloom filters to skip, sparse index to seek). **First hit wins**, because newer sources are checked first. If the first hit is a tombstone, the key is "not found."
- **SCAN:** merge the sorted streams from the memtable and all relevant SSTables, newest-wins on duplicates.

### 4.3 Crash recovery

On startup: read the **manifest** (a small file listing which SSTables currently exist), open those SSTables, then **replay the WAL** to rebuild the memtable. Now you're back exactly where you were before the crash. The test that proves this: kill the process mid-write with `kill -9`, restart, verify no committed data was lost. This test is worth its weight in gold in interviews.

**Deliverable at end of Layer 1:** a standalone, durable, crash-safe key–value store with `Get/Put/Delete/Scan`, usable as a Go library.

---

## 5. Layer 2 — Raft consensus

**Job:** make N copies of the storage engine agree on the exact same ordered sequence of operations, and keep working when nodes crash or the network misbehaves.

This is the hard, famous part. Raft is a **consensus algorithm** — a protocol for getting a group of machines to agree on a value (here, "the next operation in the log") even when some of them fail. Raft's whole selling point is that it was designed to be *understandable*, unlike its predecessor Paxos. You will implement it from the paper.

### 5.1 The three roles

At any moment, each node is one of:
- **Follower** — passive; just responds to the leader and votes in elections.
- **Candidate** — a follower who timed out waiting to hear from a leader and is now trying to become one.
- **Leader** — handles all client writes and drives replication. There's at most one leader at a time (per *term*).

### 5.2 Terms and elections

Time is divided into **terms** — numbered periods, each with at most one leader. A term is basically a logical clock that lets nodes detect stale leaders.

**Leader election:** every follower runs a **randomized election timeout** (e.g. 150–300 ms). If it doesn't hear a heartbeat from a leader before the timeout, it becomes a candidate: it increments the term, votes for itself, and asks everyone else for votes (`RequestVote` RPC). If it gets votes from a **majority** (a *quorum*), it becomes leader and starts sending heartbeats. Randomized timeouts make it unlikely that two nodes become candidates at the exact same time, which keeps elections from deadlocking.

> **Why majority?** Any two majorities of the same cluster always overlap in at least one node. That overlapping node prevents two conflicting decisions. This is why a 3-node cluster tolerates 1 failure and a 5-node cluster tolerates 2. It's also why cluster sizes are odd.

### 5.3 Log replication

The **log** is the ordered list of operations (each is a `PUT`/`DELETE`). This is the source of truth of the whole system.

1. Client sends a write to the leader.
2. Leader appends it to its own log (and persists it).
3. Leader sends `AppendEntries` RPCs to followers with the new entry.
4. Each follower runs a **consistency check** (does my log line up with the leader's just before this entry?), appends if so, and acks.
5. Once a **majority** have stored the entry, the leader marks it **committed**.
6. Committed entries get **applied** to the state machine (your Layer 1 storage engine) — in log order — on every node.

`AppendEntries` with no entries doubles as the **heartbeat** that keeps followers from starting elections.

### 5.4 The safety rules (the subtle, critical part)

Raft's correctness rests on a handful of rules that are easy to state and easy to get wrong:
- **Election restriction:** a candidate can only win if its log is at least as up-to-date as the voter's. This guarantees a new leader already has all committed entries.
- **Log Matching:** if two logs have an entry with the same index and term, everything before it is identical too.
- **Leader only commits current-term entries directly:** a leader must not consider an old-term entry committed just because it's now on a majority; it commits it indirectly by committing a current-term entry on top. (This one trips *everyone* up. Read Figure 8 of the Raft paper twice.)

### 5.5 Persistence

Certain Raft state **must** survive a crash or the algorithm breaks: `currentTerm`, `votedFor`, and the `log[]`. These get written to disk before responding to any RPC that changed them. Note this is *separate* durability from your storage engine's WAL — Raft has its own persistent state. (See the integration subtlety in Section 8.4.)

### 5.6 Snapshotting / log compaction

The Raft log grows forever if you never trim it. A **snapshot** captures the state machine's state up to some index, after which you can discard all log entries before that point. Nodes that fell too far behind get caught up with an `InstallSnapshot` RPC instead of replaying millions of log entries. For us, the snapshot *is* essentially a dump of the storage engine's state — which is why this phase ties Layers 1 and 2 together.

**Deliverable at end of Layer 2:** a cluster of N nodes that elects a leader, replicates a log, survives node crashes and network partitions, and keeps every node's storage engine identical.

---

## 6. Layer 3 — The distributed database

**Job:** wrap the Raft+storage cluster in a clean client-facing service.

### 6.1 The server and client protocol

Each node runs a network server exposing `Get/Put/Delete/Scan` to clients.

**[decision] RPC transport.** Start with Go's built-in `net/rpc` (or a simple length-prefixed protocol) to move fast, then optionally migrate to **gRPC + Protocol Buffers**. gRPC is a great résumé skill and gives you typed schemas, but it adds setup friction on day one. Either is fine; I lean "simple first, gRPC later."

### 6.2 Leader redirection

Only the leader can serve writes, but a client might contact any node. So followers reply with a hint: "I'm not the leader; the leader is Node A." The client retries against the leader. Simple, and exactly how real systems (etcd, TiKV) behave.

### 6.3 Linearizable reads — the crown jewel

Naively reading from the leader's local storage can return **stale** data: the leader might have been deposed a moment ago without realizing it, so it hands you old data. Fixing this correctly is what makes the system *linearizable* (behaves like one correct machine).

**[decision] Reads via ReadIndex.** Before serving a read, the leader (a) records its current commit index, (b) confirms it's *still* the leader by getting heartbeat acks from a majority, then (c) waits until its state machine has applied up to that index, then reads. This guarantees the read reflects everything committed before it started. Simpler-but-weaker alternatives: route reads through the log (correct but slow), or use *leader leases* (fast but relies on clock assumptions). ReadIndex is the sweet spot and a great thing to be able to explain.

### 6.4 Cluster membership

**[decision]** Start with a **static** cluster (the 3 nodes are known at boot from a config file). **Dynamic membership** (adding/removing nodes while running, via Raft's joint-consensus or single-server changes) is a stretch goal — it's genuinely tricky and best added once the core is rock-solid.

**Deliverable at end of Layer 3:** a running distributed KV database you can hammer with a client, that stays correct under failure and passes a linearizability check.

---

## 7. Build plan (phases)

Build strictly bottom-up. Each phase ends with something you can run and test. Don't move on until the current phase's tests pass — bugs in a lower layer are torture to debug through an upper one.

| Phase | What you build | You can now... |
|------:|----------------|----------------|
| **0** | Project skeleton, learn Go basics, define the `StorageEngine` and `StateMachine` interfaces | compile an empty shell |
| **1** | WAL + skiplist memtable + Get/Put | store/read durably in memory, replay WAL on restart |
| **2** | SSTables + flush + read across memtable and SSTables | data survives beyond memory; multi-source reads |
| **3** | Compaction + Bloom filters + tombstones + crash recovery | a real single-node LSM store; pass `kill -9` tests |
| **4** | Raft **leader election** (single term, no log yet) | watch a cluster elect a leader and re-elect after killing it |
| **5** | Raft **log replication** + commit + apply to storage | writes replicate to a majority and land in every store |
| **6** | Raft **persistence** + restart recovery | kill and restart nodes; cluster stays consistent |
| **7** | **Snapshotting** + InstallSnapshot (integrates L1+L2) | logs stay bounded; slow followers catch up fast |
| **8** | Client server + leader redirection + **linearizable reads** | a real DB clients can use correctly |
| **9** | Fault-injection tests, **linearizability checker**, benchmarks | *prove* it works; get numbers |
| **S** | Stretch: dynamic membership, multi-key transactions, sharding (multi-Raft) | senior-level territory |

Rough effort: Phases 1–3 are a few focused weekends. Phases 4–6 are the meat and will take longer — Raft is humbling. Phase 9 is where the project goes from "cool" to "hireable."

---

## 8. Key design decisions & tradeoffs

These are the "so why did you do it *that* way?" questions. Have an answer for each.

### 8.1 Language — **[decision] Go**
Go is the right tool: first-class concurrency (goroutines/channels map beautifully onto Raft's timers and RPCs), a great standard library for networking, garbage-collected so you focus on the algorithm not memory bugs, and the entire reference ecosystem (MIT 6.824, etcd, hashicorp/raft, TiKV's Go cousins) lives here. Your résumé shows Java/Python/JS/TS but not Go — learning it is a small, worthwhile tax and itself a plus on the résumé. **Alternative:** Rust (zero-GC, blazing, but a steeper borrow-checker climb while *also* learning distributed systems — double hard mode). If you'd rather stay in Java, it's doable but you'll fight the concurrency model more. My strong recommendation is Go. **This is the one decision to confirm before we start.**

### 8.2 LSM-tree vs B+tree — chose LSM (see 4).
### 8.3 Build Raft from scratch vs use a library — from scratch, obviously; the library *is* the project.

### 8.4 State-machine persistence — a real subtlety worth flagging now
Raft persists its *log*. Your storage engine persists its *data*. On restart you must not double-apply or skip operations. Clean approach: store Raft's `lastAppliedIndex` **inside** the storage engine, written in the same atomic batch as the data it corresponds to. On restart, the storage engine tells Raft "I've applied up to index X," and Raft replays from X+1. This keeps "what data I have" and "how far I've applied" perfectly in sync. You'll implement this in Phase 6/7 — I'm flagging it early so it's not a surprise.

### 8.5 Compaction strategy — size-tiered first, leveled as an upgrade (see 4.1).
### 8.6 Read consistency — ReadIndex (see 6.3).

---

## 9. Testing — how you'll prove it works

This section is what separates a toy from an engineering artifact. Interviewers care *more* about how you tested a distributed system than about the code.

- **Storage engine:** unit tests + **property-based tests** (generate random op sequences, compare against a reference `map`), and **crash tests** (`kill -9` mid-write, restart, assert durability).
- **Raft:** a **deterministic test harness** that simulates an unreliable network — drop messages, delay them, partition the cluster, crash and restart nodes — and asserts the safety properties never break. This mirrors the MIT 6.824 test suite; it's the gold standard.
- **Whole database:** a **linearizability checker** (the concept behind Jepsen). Record the real-time history of client operations, then verify some valid single-machine ordering explains it. In Go, the `porcupine` checker does exactly this. Passing it under injected faults is the headline result.
- **Benchmarks:** measure write/read throughput, p50/p99 latency, and behavior during a leader failover (how long is the write pause?). Numbers make the project concrete: *"~X writes/sec, p99 Y ms, recovers from leader loss in Z ms."*

Then **write it up** — a short design/results post with these numbers and the key tradeoffs. The write-up is often what gets read before the interview.

---

## 10. Definition of done & stretch goals

**Done (the core project):** a 3-node cluster that serves `Get/Put/Delete/Scan`, survives any single node crashing or being partitioned, redirects clients to the leader, serves linearizable reads, keeps bounded logs via snapshots, and passes a linearizability checker under fault injection — with published benchmarks and a write-up.

**Stretch, roughly in order of ambition:**
1. Dynamic membership changes.
2. Multi-key **transactions** (2-phase commit or percolator-style over the KV core).
3. **Sharding** — split the keyspace across multiple Raft groups (this is basically how TiKV/CockroachDB scale). This turns the project into a genuinely distributed *scalable* database and is a huge signal.
4. Leveled compaction; a block cache; MVCC.

You do **not** need the stretch goals to have an interview-dominating project. Finish the core, deeply, first.

---

## 11. Glossary

- **Key–value store:** a database that maps keys to values, like a persistent dictionary.
- **Durable:** survives a crash; committed data isn't lost.
- **WAL (Write-Ahead Log):** append-only file written before applying a change, for durability.
- **`fsync`:** OS call that forces buffered writes to the physical disk.
- **Memtable:** in-memory sorted structure holding recent writes.
- **Skiplist:** a simple probabilistic sorted structure; easier to build than a balanced tree.
- **SSTable:** immutable, sorted key–value file on disk.
- **Immutable:** never changed after creation.
- **Bloom filter:** tiny structure that quickly says "definitely not present" (or "maybe present").
- **Sparse index:** stores offsets for some keys so lookups can seek instead of scan.
- **Compaction:** background merge of SSTables that drops overwritten/deleted keys.
- **Tombstone:** a marker recording that a key was deleted.
- **Manifest:** small file tracking which SSTables currently make up the database.
- **Consensus:** getting multiple machines to agree on a value despite failures.
- **Raft:** an understandable consensus algorithm.
- **State machine replication:** replicate an *ordered log of operations*, apply it identically everywhere, get identical state.
- **Leader / Follower / Candidate:** Raft's three node roles.
- **Term:** a numbered period with at most one leader; a logical clock.
- **Election timeout:** randomized wait after which a follower tries to become leader.
- **Quorum / majority:** more than half the nodes; any two quorums overlap.
- **Log entry:** one operation in the replicated log.
- **Commit index:** highest log index known to be safely stored on a majority.
- **Applied index:** highest log index actually applied to the state machine.
- **Heartbeat:** empty `AppendEntries` the leader sends to keep followers calm.
- **Snapshot:** a saved state-machine state that lets you discard old log entries.
- **Linearizability:** the cluster behaves, from outside, like a single correct machine.
- **ReadIndex:** technique for serving linearizable reads without writing to the log.
- **RPC (Remote Procedure Call):** calling a function on another machine over the network.
- **Sharding:** splitting data across groups of nodes to scale out.

---

## 12. Learning resources (in the order you'll want them)

- **The Raft paper** — "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout). Read the *extended* version. Figures 2 and 8 are everything.
- **raft.github.io** — has an interactive visualization; watch elections happen.
- **MIT 6.824 (Distributed Systems)** — lectures and labs are online; the Raft labs are essentially Phases 4–8 of this doc.
- **CMU 15-445 (Database Systems)** — for the storage-engine internals.
- **"Database Internals"** by Alex Petrov — LSM-trees, B-trees, storage. Perfect for Layer 1.
- **"Designing Data-Intensive Applications"** by Martin Kleppmann — the big-picture bible for everything here.
- **LevelDB / RocksDB docs & source** — a real LSM-tree to compare notes with.
- **`porcupine`** (Go) — the linearizability checker for Phase 9.

---

*Next step: confirm the language (Section 8.1), then we spec out Phase 1 — the WAL and memtable — in detail and start writing code.*
