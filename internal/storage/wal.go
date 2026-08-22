package storage

import (
	"bufio"
	"io"
	"os"
)

// WAL is the write-ahead log: an append-only file on disk. Every mutation is
// written here (and fsync'd) BEFORE it is applied to the memtable. On restart
// we replay the WAL to rebuild the memtable exactly as it was.
//
// On-disk record format (integers are big-endian) — identical to the SSTable
// DATA entry, so both go through encodeRecord/decodeRecord in codec.go:
//
//	+--------+---------+---------+-----------+-----------+
//	| kind   | keyLen  | key     | valueLen  | value     |
//	| 1 byte | 4 bytes | keyLen  | 4 bytes   | valueLen  |
//	+--------+---------+---------+-----------+-----------+
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
func (l *WAL) Append(r record) error {
	_, err := encodeRecord(l.w, r)
	return err
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
func Replay(path string, fn func(record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		rec, err := decodeRecord(r)
		if err == io.EOF {
			return nil // clean end: we read every complete record
		}
		if err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
	}
}

// Close flushes any buffered bytes and closes the file.
func (l *WAL) Close() error {
	if err := l.w.Flush(); err != nil {
		return err
	}
	return l.f.Close()
}
