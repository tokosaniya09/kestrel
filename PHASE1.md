# Phase 1 — Build Guide: WAL + Memtable

**Goal of this phase:** a durable, single-node key-value store. You can `Put`,
`Get`, `Delete`, and — the important part — everything survives a process
restart. When `go test ./...` is green, Phase 1 is done.

This guide assumes the scaffold from `kestrel-phase1.zip`.

---

## Step 0 — Set up Go (once)

1. Install Go 1.22+ from https://go.dev/dl/ and check it:
   ```bash
   go version
   ```
2. Unzip `kestrel-phase1.zip` and enter it:
   ```bash
   unzip kestrel-phase1.zip && cd kestrel
   ```
3. Run the tests. They'll fail — that's expected, three functions are stubbed:
   ```bash
   go test ./...
   ```
   You'll see panics like `TODO: implement Memtable.Get`. Your job this phase is
   to replace those three stubs until the tests pass.

---

## The directory, and where you write code

```
kestrel/
├── go.mod                     ← module definition (done)
├── cmd/kestrel/main.go        ← a REPL to poke the store by hand (done)
└── internal/storage/
    ├── storage.go             ← Engine interface + types            (done, read it)
    ├── memtable.go            ← YOU implement 4 stubs here    ← START HERE
    ├── wal.go                 ← YOU implement 2 stubs here    ← THEN HERE
    ├── db.go                  ← ties it together (done, read it)
    └── db_test.go             ← the spec; don't edit, just pass it
```

You only touch **two files** this phase: `memtable.go` then `wal.go`. Everything
else is written for you so you can see how the pieces fit.

---

## Step 1 — Implement the memtable (`internal/storage/memtable.go`)

The memtable is just the freshest data, held in memory. Start with the simplest
thing that works: a `map[string]record`.

Fill in the four stubs:

- **the struct** — give it one field, e.g. `data map[string]record`.
- **`NewMemtable`** — return `&Memtable{data: make(map[string]record)}`.
- **`Put`** — `m.data[string(key)] = record{kind: opPut, key: key, value: value}`.
- **`Delete`** — store a **tombstone**: `record{kind: opDelete, key: key}`.
  Do *not* call Go's built-in `delete()` on the map. A tombstone is a real
  stored record; later phases need it to shadow older values on disk.
- **`Get`** — look up `m.data[string(key)]`. If it's missing, return
  `(nil, false)`. If it's there but `kind == opDelete`, also return
  `(nil, false)`. Otherwise return `(rec.value, true)`.

That's the whole memtable for now. Run `go test ./...` — `TestPutGet`,
`TestOverwrite`, and `TestDelete` should now pass. `TestDurabilityAcrossRestart`
will still fail, because nothing is on disk yet. That's next.

> Why `string(key)` as the map key? Go maps can't use `[]byte` as a key (slices
> aren't comparable), so we convert to `string` for the lookup. The bytes are
> the same.

---

## Step 2 — Implement the WAL (`internal/storage/wal.go`)

The write-ahead log is what makes data survive a restart. `db.go` already calls
`wal.Append` then `wal.Sync` on every write (log first, then memory — the
"write-ahead" rule), and calls `Replay` at startup. You implement the encoding.

**The on-disk format for one record** (already documented in the file):

```
+--------+---------+---------+-----------+-----------+
| kind   | keyLen  | key     | valueLen  | value     |
| 1 byte | 4 bytes | keyLen  | 4 bytes   | valueLen  |
+--------+---------+---------+-----------+-----------+
```

### `Append(r record) error` — write one record

Add `import "encoding/binary"` and write, in order, to `l.w`:

1. `r.kind` (1 byte) — `l.w.WriteByte(byte(r.kind))`
2. `uint32(len(r.key))` — `binary.Write(l.w, binary.BigEndian, uint32(len(r.key)))`
3. the key bytes — `l.w.Write(r.key)`
4. `uint32(len(r.value))`
5. the value bytes — `l.w.Write(r.value)`

Check each error and return the first non-nil one.

### `Replay(path string, fn func(record) error) error` — read them all back

Add `import ("encoding/binary"; "io")`. Open `path` read-only, wrap it in a
`bufio.NewReader`, and loop:

1. Read the 1-byte kind with `r.ReadByte()`. If it returns `io.EOF`, you've
   read the whole file cleanly — `return nil`.
2. Read `keyLen` (uint32) with `binary.Read`, then read exactly that many bytes
   with `io.ReadFull` into a `make([]byte, keyLen)`.
3. Read `valueLen` and its bytes the same way.
4. Call `fn(record{kind: op(kind), key: key, value: value})`; if it errors,
   return that error.

Remember to close the file (a `defer f.Close()` after opening).

Run `go test ./...` again. `TestDurabilityAcrossRestart` should now pass,
because `Open` replays the WAL into a fresh memtable. **All green = Phase 1 done.**

---

## Step 3 — See it with your own eyes

```bash
go run ./cmd/kestrel
> put name kestrel
> put city gwalior
> get name
kestrel
> exit
```

Now run it **again**:

```bash
go run ./cmd/kestrel
> get name
kestrel        ← it persisted, because the WAL was replayed on startup
```

Open `./data/wal.log` in a hex viewer (`xxd data/wal.log | head`) and you can
literally see your records on disk. That "oh, it's really just bytes in a file"
moment is the point of Phase 1.

---

## What you'll have learned

- Why durability = "append to a log and fsync **before** you touch memory".
- What a tombstone is and why append-only stores can't just erase.
- How crash recovery works: replay the log to rebuild in-memory state.
- Basic binary encoding with `encoding/binary` (you'll reuse this for SSTables
  and later for Raft's on-disk log).

---

## Stretch (optional, only if you're enjoying it)

- **CRC checksums:** prepend a 4-byte CRC32 to each record and verify it on
  replay, so a half-written trailing record (from a real crash) is detected and
  skipped rather than corrupting recovery. This is exactly what real WALs do.
- **Skiplist memtable:** swap the map for a skiplist so keys stay sorted. You
  don't need this until Phase 2 (SSTable flush needs sorted iteration), but it's
  a satisfying self-contained exercise if you want it now.

Don't gold-plate, though — a passing Phase 1 is the win. Ship it, then we spec
**Phase 2: SSTables and flushing to disk**.

---

## When you're stuck

Tell me which test is failing and paste the error. The most common Phase 1 bugs:
- forgetting to `Sync` (data never hits disk — restart test fails),
- using Go's `delete()` instead of a tombstone (delete "works" now but breaks
  Phase 2),
- an off-by-one in the encoding (a length written as the wrong width) — reading
  back garbage. Hex-dump the WAL to debug.
