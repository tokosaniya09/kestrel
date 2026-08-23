# Kestrel

A distributed, replicated, crash-safe key-value database, built from scratch.
See `DESIGN.md` for the full plan.

**Current state: Phase 3a** (Layer 1 — storage engine). Done: WAL + skiplist
memtable (Phase 1), SSTables + flushing + multi-level reads (Phase 2).
In progress: compaction (Phase 3a). Next: Bloom filters (3b), manifest (3c).

## Prerequisites

Install Go 1.22+ from https://go.dev/dl/ and confirm: `go version`

## Run the tests

```bash
go test ./...
```

The Phase 1/2 tests pass already. The Phase 3a tests fail until you implement
`mergeSSTables` in `internal/storage/merge.go` — see `PHASE3.md`.

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
├── PHASE3.md                    current phase build guide
├── cmd/kestrel/main.go          REPL (put/get/del/flush/compact/exit)
└── internal/storage/            Layer 1 — the storage engine
    ├── storage.go               Engine interface + types      (done)
    ├── codec.go                 shared record encode/decode   (done)
    ├── memtable.go              skiplist memtable             (done)
    ├── wal.go                   write-ahead log               (done)
    ├── sstable.go               SSTable read/write            (done, Phase 2)
    ├── db.go                    flush + multi-level reads     (done)
    ├── compaction.go            SSTable iterator + Compact()  (done)
    ├── merge.go                 k-way merge            (TODO: you, Phase 3a)
    ├── sstable_test.go          isolated SSTable spec         (done)
    ├── db_test.go               Phase 1 + Phase 2 tests       (done)
    └── compaction_test.go       Phase 3a tests                (done)
```
