package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"kestrel/internal/storage"
)

// A tiny REPL to poke the storage engine by hand. Commands:
//
//	put <key> <value>
//	get <key>
//	del <key>
//	flush            force the memtable out to a new .sst file
//	compact          merge all .sst files into one, dropping dead data
//	exit
//
// Data is written to ./data so you can quit, restart, and confirm it persisted.
func main() {
	db, err := storage.Open("./data")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fmt.Println("kestrel storage REPL — commands: put / get / del / flush / compact / exit")
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			if err := sc.Err(); err != nil {
				fmt.Println("error reading input:", err)
			}
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
			if err := db.Put([]byte(parts[1]), []byte(parts[2])); err != nil {
				fmt.Println("error:", err)
			}
		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			v, found, err := db.Get([]byte(parts[1]))
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
			if err := db.Delete([]byte(parts[1])); err != nil {
				fmt.Println("error:", err)
			}
		case "flush":
			if err := db.Flush(); err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Println("flushed memtable to an sstable (see ./data/*.sst)")
			}
		case "compact":
			if err := db.Compact(); err != nil {
				fmt.Println("error:", err)
			} else {
				fmt.Println("compacted all sstables into one (dead data reclaimed)")
			}
		case "exit", "quit":
			return
		default:
			fmt.Println("unknown command:", parts[0])
		}
	}
}
