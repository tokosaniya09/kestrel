# Kestrel

A distributed, replicated, crash-safe key-value database, built from scratch.
See `DESIGN.md` for the full plan.

**Current state: Phase 4** (Layer 2 — Raft). Layer 1 storage engine done
(P1 WAL+memtable, P2 SSTables, P3a compaction, P3b Bloom filters; P3c manifest
deferred). In progress: Raft leader election (P4). See PROGRESS.md.

## Prerequisites

Install Go 1.22+ from https://go.dev/dl/ and confirm: `go version`

## Run the tests

```bash
go test ./...
```

The Phase 1/2 tests pass already. The Phase 3a tests fail until you implement
the Bloom filter methods in `internal/storage/bloom.go` — see `PHASE3B.md`.

## Try it by hand

```bash
go run ./cmd/kestrel
# > put a 1
# > flush
# > put a 2
# > flush
# > compact        # merges all .sst files into one, drops dead data
# > get a
# 2
# > exit
```

## Layout

```
kestrel/
├── go.mod
├── DESIGN.md                    full project design doc
├── PHASE3.md                    current phase build guide (see also PHASE3.md, PROGRESS.md)
├── cmd/kestrel/main.go          REPL (put/get/del/flush/compact/exit)
└── internal/storage/            Layer 1 — the storage engine
    ├── storage.go               Engine interface + types      (done)
    ├── codec.go                 shared record encode/decode   (done)
    ├── memtable.go              skiplist memtable             (done)
    ├── wal.go                   write-ahead log               (done)
    ├── sstable.go               SSTable read/write            (done, Phase 2)
    ├── db.go                    flush + multi-level reads     (done)
    ├── compaction.go            SSTable iterator + Compact()  (done)
    ├── merge.go                 k-way merge                   (done, P3a)
    ├── bloom.go                 Bloom filter           (TODO: you, P3b)
    ├── sstable_test.go          isolated SSTable spec         (done)
    ├── db_test.go               Phase 1 + Phase 2 tests       (done)
    └── compaction_test.go       Phase 3a tests                (done)
```
