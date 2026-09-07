# Running a Kestrel Cluster

Everything built so far has only ever run inside `go test`. This guide starts
a **real cluster of separate OS processes** and talks to it from an external
client — the first time Kestrel actually runs as a program rather than as a
test fixture.

---

## Build

From the project root:

```powershell
go build -o bin/kestrel-server.exe ./cmd/kestrel-server
go build -o bin/kestrel-cli.exe ./cmd/kestrel-cli
```

(On macOS/Linux, drop the `.exe`.)

---

## Start a 3-node cluster

Open **three separate terminals**, one per node. Every node gets the **same**
`-peers` list but a **different** `-id` and `-data` directory.

Terminal 1:
```powershell
.\bin\kestrel-server.exe -id 0 -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002 -data .\clusterdata\node0
```

Terminal 2:
```powershell
.\bin\kestrel-server.exe -id 1 -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002 -data .\clusterdata\node1
```

Terminal 3:
```powershell
.\bin\kestrel-server.exe -id 2 -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002 -data .\clusterdata\node2
```

**Watch what happens as you start them.** After the first node, nothing much:
it can't reach a majority (1 of 3), so it keeps timing out and starting
elections it can't win. That's correct — it's the `TestNoLeaderWithoutMajority`
behavior you wrote a test for back in Phase 4, now visible live. Once the
**second** node comes up, 2 of 3 is a majority and a leader gets elected almost
immediately.

---

## Talk to it

A fourth terminal:

```powershell
.\bin\kestrel-cli.exe -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002
```

```
kestrel cli — 3 nodes known. commands: put / get / del / exit
> put name toko
ok
> get name
toko
> del name
ok
> get name
(not found)
```

The client has no idea which node is the leader when it starts. It picks one,
and if that's not the leader, that node replies "not me, try node N" and the
client follows the redirect — the Phase 10 machinery, working for real over
TCP.

---

## Things worth actually trying

These are the demos that show off what you built. Each one is a
question an interviewer might ask, answered by doing it.

### 1. Data really is replicated

Put a key via the CLI, then **kill the leader** (Ctrl-C in whichever terminal
says it's leading — or just try one and watch the CLI's behavior). Wait a
couple of seconds for a new election, then `get` your key again through the
CLI. It's still there: the surviving nodes each had their own copy all along.

### 2. Writes survive a full cluster restart

Put a few keys. Ctrl-C **all three** nodes. Start them all again with the same
`-data` directories. `get` your keys — still there. That's the WAL, SSTables,
and the persisted Raft log (Phase 6's `FilePersister`, getting its first real
non-test use here) all doing their jobs across process death.

### 3. A minority can't accept writes

Kill **two** of the three nodes. Try a `put` from the CLI — it fails after
exhausting its retries. The one surviving node cannot reach a majority, so it
cannot commit anything. This is Raft refusing to accept a write it can't
guarantee, which is exactly what you want. Restart a second node and the same
`put` succeeds.

### 4. Look at what's actually on disk

```powershell
Get-ChildItem -Recurse .\clusterdata\node0
```

You'll find `raft.state` (the persisted term/vote/log) and a `store/`
directory with `wal.log` and any `.sst` files. Every node has its own
complete, independent copy — five nodes would mean five full databases, kept
in sync by Raft. `Format-Hex` still works on those `.sst` files, exactly as it
did back in Phase 2.

---

## What this doesn't do yet

- **Reads can be stale.** `get` is answered by whichever node the client
  happens to be talking to, from its local state — that node may be slightly
  behind the leader. This is the deliberate Phase 8 design decision, and
  ReadIndex (Phase 11) is the fix.
- **Static membership.** The `-peers` list is fixed at startup. Adding or
  removing a node means restarting everything with a new list.
- **No authentication or TLS.** Plaintext TCP, fine for localhost.
- **A node's own `flush`/`compact` aren't exposed.** Those are local
  storage-engine operations; the CLI speaks to the cluster, not to one node's
  internals. Compaction currently only happens if triggered internally.

---

## Troubleshooting

**"connection refused" / the CLI can't reach anything** — check that at least
two of the three servers are actually running. A single node can't do
anything useful.

**A node exits immediately** — it probably can't bind its port. Check nothing
else is on 9000-9002, and that the `-id` matches an entry in `-peers`.

**Writes fail but reads work** — that's the minority case (#3 above). Reads
are served locally by any node; writes need a majority.
