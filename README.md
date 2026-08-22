# Kestrel

A distributed, replicated, crash-safe key-value database, built from scratch.
See `DESIGN.md` for the full plan. This repo is at **Phase 1** (Layer 1: the
write-ahead log + memtable).

## Prerequisites

Install Go 1.22+ from https://go.dev/dl/ and confirm:

```bash
go version
```

## Run the tests (your Phase 1 to-do list)

The tests in `internal/storage/db_test.go` are the spec. They fail right now
because three functions are stubbed with `panic("TODO...")`. Implement them and
the tests go green:

```bash
go test ./...
```

## Try it by hand

Once the tests pass, run the REPL:

```bash
go run ./cmd/kestrel
# > put name kestrel
# > get name
# kestrel
# > exit
# ...run it again and `get name` still works — that's the WAL doing its job.
```

## Layout

```
kestrel/
├── go.mod                       module definition
├── cmd/
│   └── kestrel/
│       └── main.go              a tiny REPL to poke the store by hand
└── internal/
    └── storage/                 Layer 1 — the storage engine
        ├── storage.go           Engine interface + shared types  (done)
        ├── memtable.go          in-memory layer                  (TODO: you)
        ├── wal.go               write-ahead log                  (TODO: you)
        ├── db.go                wires WAL + memtable together     (done)
        └── db_test.go           the Phase 1 spec / tests         (done)
```

`internal/` is a Go convention: packages under it can only be imported by code
in this module, which keeps the public surface clean.
