package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"kestrel/internal/client"
	"kestrel/internal/config"
)

// kestrel-cli talks to a running Kestrel cluster from OUTSIDE it — it is not
// a cluster member, just a client that knows everyone's address and finds the
// leader by following redirects.
//
//	kestrel-cli -peers 0=127.0.0.1:9000,1=127.0.0.1:9001,2=127.0.0.1:9002
//
// Then, at the prompt: put <key> <value> / get <key> / del <key> / exit
//
// Note there's no `flush` or `compact` here, unlike the old single-node REPL:
// those are local storage-engine operations, and a client speaks to the
// cluster as a whole, not to one node's internals.
func main() {
	peersArg := flag.String("peers", "", "comma-separated id=host:port list")
	flag.Parse()

	if *peersArg == "" {
		flag.Usage()
		os.Exit(2)
	}

	addrs, err := config.ParsePeers(*peersArg)
	if err != nil {
		log.Fatalf("invalid -peers: %v", err)
	}

	c := client.New(addrs)
	fmt.Printf("kestrel cli — %d nodes known. commands: put / get / del / exit\n", len(addrs))

	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			return
		}
		parts := strings.Fields(sc.Text())
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "put":
			if len(parts) != 3 {
				fmt.Println("usage: put <key> <value>")
				continue
			}
			if err := c.Put([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Println("ok")
			}

		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			v, found, err := c.Get([]byte(parts[1]))
			if err != nil {
				fmt.Println("error:", err)
			} else if !found {
				fmt.Println("(not found)")
			} else {
				fmt.Printf("%s\n", v)
			}

		case "del":
			if len(parts) != 2 {
				fmt.Println("usage: del <key>")
				continue
			}
			if err := c.Delete([]byte(parts[1])); err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Println("ok")
			}

		case "exit", "quit":
			return

		default:
			fmt.Println("unknown command:", parts[0])
		}
	}
}
