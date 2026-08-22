# Kestrel

A distributed, replicated, crash-safe key-value database, built from scratch.
See `DESIGN.md` for the full plan.

**Current state: Phase 2** (Layer 1 — storage engine: WAL + skiplist memtable +
SSTables). Phase 1 (WAL + memtable) is complete; Phase 2 adds flushing to
on-disk SSTables and a multi-level read path.

## Prerequisites

Install Go 1.22+ from https://go.dev/dl/ and confirm: `go version`

## Run the tests

```bash
go test ./...
```

The Phase 1 tests pass already. The Phase 2 tests fail until you implement the
four stubs in `internal/storage/sstable.go` — see `PHASE2.md`.

## Try it by hand

```bash
go run ./cmd/kestrel
# > put a 1
# > flush          # writes ./data/000000.sst
# > get a
# 1
# > exit
```

## Layout

```
kestrel/
├── go.mod
├── DESIGN.md                    full project design doc
├── PHASE2.md                    current phase build guide
├── cmd/kestrel/main.go          REPL (put/get/del/flush/exit)
└── internal/storage/            Layer 1 — the storage engine
    ├── storage.go               Engine interface + types      (done)
    ├── codec.go                 shared record encode/decode   (done)
    ├── memtable.go              skiplist memtable             (done)
    ├── wal.go                   write-ahead log               (done)
    ├── db.go                    flush + multi-level reads     (done)
    ├── sstable.go               SSTable read/write        (TODO: you)
    ├── sstable_test.go          isolated SSTable spec         (done)
    └── db_test.go               Phase 1 + Phase 2 tests       (done)
```
