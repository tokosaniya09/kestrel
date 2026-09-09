# Kestrel

A distributed, replicated, crash-safe key-value database built from scratch in
Go — no external dependencies, standard library only.

Three layers, built bottom-up:

1. **Storage engine** — an LSM-tree: write-ahead log, skip-list memtable,
   sorted SSTables with in-file indexes, background compaction, and per-file
   Bloom filters.
2. **Consensus** — Raft implemented from scratch: leader election, log
   replication, crash-safe persistence, and snapshots.
3. **Distributed layer** — each node pairs Raft with its own private storage
   engine as a replicated state machine, exposed over TCP RPC, with a client
   that finds the leader by following redirects.

## Quick start

```bash
go build -o bin/kestrel-server ./cmd/kestrel-server
go build -o bin/kestrel-cli ./cmd/kestrel-cli
```

Start three nodes, each in its own terminal, with the same `-peers` list but a
different `-id` and `-data` directory:

```bash
./bin/kestrel-server -id 0 -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002 -data ./clusterdata/node0
./bin/kestrel-server -id 1 -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002 -data ./clusterdata/node1
./bin/kestrel-server -id 2 -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002 -data ./clusterdata/node2
```

Then talk to the cluster:

```bash
./bin/kestrel-cli -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002
```

```
> put name toko
ok
> get name
toko
```

`RUNNING.md` has a fuller walkthrough, including how to watch a leader
failover and confirm data survives a full cluster restart.

## Tests

```bash
go test ./...
```

The consensus tests use an in-memory network to simulate node crashes and
network partitions. Because elections and replication are timing-dependent,
running them repeatedly is worthwhile:

```bash
go test -count=10 ./...
```

## Layout

```
kestrel/
├── cmd/
│   ├── kestrel-server/     one cluster node
│   ├── kestrel-cli/        external client REPL
│   └── kestrel/            single-node storage engine REPL
└── internal/
    ├── storage/            LSM-tree: WAL, memtable, SSTables, compaction, Bloom filters
    ├── raft/               consensus: election, replication, persistence, snapshots
    ├── node/               Raft + storage engine wired as a state machine
    ├── rpc/                TCP transport and client-facing RPC service
    ├── client/             leader-following client
    └── config/             peer-list parsing
```

## Design

`DESIGN.md` covers the architecture and the reasoning behind the major
decisions — LSM-tree over B+tree, implementing Raft rather than using a
library, and the storage and consensus data formats.

## Current limitations

- **Reads default to eventual consistency.** `Get` is answered by any node from
  its local state and may lag the leader. `LinearizableGet` provides the strong
  guarantee via the leader's ReadIndex protocol, at the cost of one heartbeat
  round-trip.
- **Static membership.** The peer list is fixed at startup; nodes can't join or
  leave a running cluster.
- **Received snapshots aren't installed into the state machine.** A node that
  falls far behind gets Raft's bookkeeping updated but recovers its data by
  replaying its own persisted log.
- **Compaction has a crash window.** A crash between writing a merged SSTable
  and deleting the originals leaks the old files. Reads stay correct; a manifest
  recording the live SSTable set would close the window.
- **No TLS or authentication.** Plaintext TCP.
