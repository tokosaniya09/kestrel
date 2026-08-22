package storage

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

// WAL is the write-ahead log: an append-only file on disk. Every mutation is
// written here (and fsync'd) BEFORE it is applied to the memtable. On restart
// we replay the WAL to rebuild the memtable exactly as it was.
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
func (l *WAL) Append(r record) error {
	if err := l.w.WriteByte(byte(r.kind)); err != nil {
		return err
	}
	if err := binary.Write(l.w, binary.BigEndian, uint32(len(r.key))); err != nil {
		return err
	}
	if _, err := l.w.Write(r.key); err != nil {
		return err
	}
	if err := binary.Write(l.w, binary.BigEndian, uint32(len(r.value))); err != nil {
		return err
	}
	if _, err := l.w.Write(r.value); err != nil {
		return err
	}
	return nil
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
		// Read the 1-byte kind. A clean EOF here means we've read every
		// complete record — that's the normal way this loop ends.
		kind, err := r.ReadByte()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		var keyLen uint32
		if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
			return err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}

		var valueLen uint32
		if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
			return err
		}
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(r, value); err != nil {
			return err
		}

		if err := fn(record{kind: op(kind), key: key, value: value}); err != nil {
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