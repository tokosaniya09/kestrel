package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"kestrel/internal/config"
	"kestrel/internal/node"
	"kestrel/internal/raft"
	"kestrel/internal/rpc"
	"kestrel/internal/storage"
)

// kestrel-server runs ONE node of a Kestrel cluster as a real OS process.
//
// Start a 3-node cluster in three terminals:
//
//	kestrel-server -id 0 -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002 -data ./data/node0
//	kestrel-server -id 1 -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002 -data ./data/node1
//	kestrel-server -id 2 -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002 -data ./data/node2
//
// Every node takes the SAME -peers list (static membership) but a different
// -id and -data directory. Until a majority are up, the cluster can't elect a
// leader and won't accept writes — that's correct Raft behavior, not a bug.
func main() {
	var (
		id       = flag.Int("id", -1, "this node's id (must appear in -peers)")
		peersArg = flag.String("peers", "", "comma-separated id=host:port list, e.g. 0=127.0.0.1:9000,1=127.0.0.1:9001")
		dataDir  = flag.String("data", "", "directory for this node's storage engine and Raft state")
	)
	flag.Parse()

	if *id < 0 || *peersArg == "" || *dataDir == "" {
		flag.Usage()
		os.Exit(2)
	}

	addrs, err := config.ParsePeers(*peersArg)
	if err != nil {
		log.Fatalf("invalid -peers: %v", err)
	}
	myAddr, ok := addrs[*id]
	if !ok {
		log.Fatalf("id %d does not appear in -peers", *id)
	}

	peers := make([]int, 0, len(addrs))
	for peerID := range addrs {
		peers = append(peers, peerID)
	}

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("creating data dir: %v", err)
	}

	// The storage engine: this node's own private copy of the state machine.
	db, err := storage.Open(filepath.Join(*dataDir, "store"))
	if err != nil {
		log.Fatalf("opening storage engine: %v", err)
	}
	defer db.Close()

	// Real disk persistence for Raft state — FilePersister's first use outside
	// of a test (every test so far used MemoryPersister). This is what makes a
	// restarted process remember its term, vote, and log.
	persister := raft.NewFilePersister(filepath.Join(*dataDir, "raft.state"))

	transport := rpc.NewRPCTransport(addrs)
	r := raft.NewRaft(*id, peers, transport, persister)
	n := node.NewNode(r, db)

	listener, err := rpc.ServeNode(r, n, myAddr)
	if err != nil {
		log.Fatalf("starting RPC server on %s: %v", myAddr, err)
	}
	defer listener.Close()

	r.Start()

	log.Printf("kestrel node %d listening on %s (cluster of %d, data in %s)",
		*id, myAddr, len(addrs), *dataDir)
	log.Printf("waiting for a majority of nodes to come up before a leader can be elected")

	// Block until Ctrl-C, then shut down cleanly: stop Raft first so no apply
	// loop is mid-write when the storage engine closes (the deferred Close
	// calls above run in reverse order — listener, then db).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Printf("shutting down node %d", *id)
	r.Stop()
}

