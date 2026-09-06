# Kestrel

A distributed, replicated, crash-safe key-value database, built from scratch.
See `DESIGN.md` for the full plan and `PROGRESS.md` for detailed status.

**Current state: Phase 8** (Layer 3 begins). Layer 1 (storage) and Layer 2
(Raft: election, replication, persistence, snapshots) complete. In progress:
wiring Raft's committed log to the storage engine as a real state machine
(internal/node) — a working replicated KV store, minus networking and
linearizable reads (later phases). See PROGRESS.md for full status.

## Prerequisites

Install Go 1.22+ from https://go.dev/dl/ and confirm: `go version`

## Run the tests

```bash
go test ./...
```

Storage engine and Raft phases 4-6 pass already. Phase 7 tests fail until you:
1. Apply the two required edits to your existing replication.go/persistence.go
   (PHASE7.md "Step 0").
2. Implement the four stubs in `internal/raft/snapshot.go`.

See `PHASE7.md` for the full guide.

## Layout

```
kestrel/
├── go.mod
├── DESIGN.md                    full project design doc
├── PROGRESS.md                  detailed phase-by-phase status (read this first
│                                 if resuming after a break)
├── PHASE7.md                    current phase build guide
├── cmd/kestrel/main.go          REPL (put/get/del/flush/compact/exit)
├── internal/storage/            Layer 1 — the storage engine (complete)
│   ├── storage.go, codec.go, memtable.go, wal.go, sstable.go, db.go,
│   ├── compaction.go, merge.go, bloom.go
│   └── *_test.go
└── internal/raft/               Layer 2 — Raft consensus (in progress)
    ├── rpc.go, log.go, raft.go, election.go, replication.go, snapshot.go,
    ├── persister.go, persistence.go
    └── *_test.go
```
