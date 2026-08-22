package storage

import (
	"bufio"
	"os"
)

// WAL is the write-ahead log: an append-only file on disk. Every mutation is
// written here (and fsync'd) BEFORE it is applied to the memtable. That
// ordering is the entire durability guarantee — on restart we replay the WAL
// to rebuild the memtable exactly as it was before the crash.
//
// On-disk record format (integers are big-endian):
//
//	+--------+---------+---------+-----------+-----------+
//	| kind   | keyLen  | key     | valueLen  | value     |
//	| 1 byte | 4 bytes | keyLen  | 4 bytes   | valueLen  |
//	+--------+---------+---------+-----------+-----------+
//
// For a delete (tombstone), kind = opDelete and valueLen = 0.
type WAL struct {
	f *os.File
	w *bufio.Writer
}

// OpenWAL opens (creating if needed) the WAL file for appending.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, w: bufio.NewWriter(f)}, nil
}

// Append writes one record into the buffer. It is NOT durable until Sync.
//
// TODO(you): encode r in the format documented above and write it to l.w.
// Hints:
//   - import "encoding/binary"
//   - binary.Write(l.w, binary.BigEndian, r.kind) writes the 1-byte kind
//   - write uint32(len(r.key)), then l.w.Write(r.key)
//   - write uint32(len(r.value)), then l.w.Write(r.value)
//   - return the first error you hit
func (l *WAL) Append(r record) error {
	panic("TODO: implement WAL.Append")
}

// Sync flushes the buffer and fsyncs the file, forcing the OS to persist the
// bytes to physical disk. This is what makes a write survive a crash.
func (l *WAL) Sync() error {
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Sync()
}

// Replay reads the whole WAL at path from the beginning and calls fn for each
// record in write order. Used at startup to rebuild the memtable.
//
// TODO(you): open path read-only, wrap it in a bufio.Reader, and loop:
//   - import "encoding/binary" and "io"
//   - read the 1-byte kind; a clean io.EOF here means "done" -> return nil
//   - read uint32 keyLen, then that many bytes (io.ReadFull)
//   - read uint32 valueLen, then that many bytes
//   - call fn(record{kind, key, value})
func Replay(path string, fn func(record) error) error {
	panic("TODO: implement Replay")
}

// Close flushes any buffered bytes and closes the file.
func (l *WAL) Close() error {
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Close()
}
