package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

// SSTable is an immutable, sorted key-value file on disk, produced by flushing a
// memtable. This is the core of Phase 2 and the file YOU implement.
//
// File layout (integers big-endian):
//
//  DATA    : records in ascending key order, each written by encodeRecord
//            (codec.go): [kind:1][keyLen:4][key][valLen:4][value]
//  INDEX   : one entry per key, ascending: [keyLen:4][key][offset:8]
//            where offset is the byte position of that key's record in DATA.
//  FOOTER  : fixed 20 bytes at the very end of the file:
//            [indexOffset:8][indexLen:8][magic:4]
//
// Reading works backwards: read the 20-byte footer to find the index, load the
// index into memory, then binary-search it and seek into DATA for a hit.
//
// See PHASE2.md for the full step-by-step for each TODO below.

// sstMagic marks a valid SSTable footer ("KEST" in ASCII). If the bytes you read
// back don't match, you're not looking at a real footer — bail out.
const sstMagic uint32 = 0x4B455354

// indexEntry maps a key to the byte offset of its record in the DATA section.
type indexEntry struct {
	key    []byte
	offset int64
}

// SSTable is an open, readable SSTable. It keeps the file handle open and the
// index resident in memory for fast point lookups.
type SSTable struct {
	f     *os.File
	index []indexEntry // ascending by key
}

// writeSSTable writes every record of m (ForEach yields them in sorted order) to
// a new SSTable at path. Write to a temp file and os.Rename it into place at the
// end, so a crash can never leave a half-written .sst that OpenSSTable would choke on.
func writeSSTable(path string, m *Memtable) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)

	// --- DATA section: records in sorted order, remembering each offset ---
	var index []indexEntry
	var offset int64
	var writeErr error
	m.ForEach(func(r record) {
		if writeErr != nil {
			return
		}

		// copy the key: we hold onto it for the index after ForEach returns
		k := append([]byte(nil), r.key...)
		index = append(index, indexEntry{key: k, offset: offset})

		n, err := encodeRecord(w, r)
		if err != nil {
			writeErr = err
			return
		}

		offset += int64(n)
	})

	if writeErr != nil {
		return writeErr
	}

	// --- INDEX section: [kLen][key][offset] per entry ---
	indexOffset := offset
	var indexLen int64

	for _, e := range index {
		if err := binary.Write(w, binary.BigEndian, uint32(len(e.key))); err != nil {
			return err
		}

		if _, err := w.Write(e.key); err != nil {
			return err
		}

		if err := binary.Write(w, binary.BigEndian, e.offset); err != nil {
			return err
		}

		indexLen += int64(4 + len(e.key) + 8)
	}

	// --- FOOTER: indexOffset, indexLen, magic (20 bytes total) ---
	if err := binary.Write(w, binary.BigEndian, indexOffset); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, indexLen); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, sstMagic); err != nil {
		return err
	}

	// flush buffer, fsync, close, then atomically publish the file
	if err := w.Flush(); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

// OpenSSTable opens an existing SSTable file and loads its index into memory.
func OpenSSTable(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	// Read the 20-byte footer from the end. ReadAt doesn't move the file offset.
	footer := make([]byte, 20)
	if _, err := f.ReadAt(footer, fi.Size()-20); err != nil {
		f.Close()
		return nil, err
	}

	indexOffset := int64(binary.BigEndian.Uint64(footer[0:8]))
	indexLen := int64(binary.BigEndian.Uint64(footer[8:16]))
	magic := binary.BigEndian.Uint32(footer[16:20])

	if magic != sstMagic {
		f.Close()
		return nil, fmt.Errorf("bad sstable magic in %s", path)
	}

	// Read the whole index section and parse it into memory.
	buf := make([]byte, indexLen)
	if _, err := f.ReadAt(buf, indexOffset); err != nil {
		f.Close()
		return nil, err
	}

	var index []indexEntry

	for len(buf) > 0 {
		klen := binary.BigEndian.Uint32(buf[0:4])
		buf = buf[4:]

		key := append([]byte(nil), buf[:klen]...)
		buf = buf[klen:]

		off := int64(binary.BigEndian.Uint64(buf[0:8]))
		buf = buf[8:]

		index = append(index, indexEntry{key: key, offset: off})
	}

	return &SSTable{f: f, index: index}, nil
}

// Get returns the record for key and whether THIS SSTable contains it. A
// tombstone counts as present: return (rec, true, nil) with rec.kind == opDelete
// so the caller knows the key is deleted and must not fall through to older files.
func (s *SSTable) Get(key []byte) (record, bool, error) {
	i := sort.Search(len(s.index), func(i int) bool {
		return bytes.Compare(s.index[i].key, key) >= 0
	})

	if i >= len(s.index) || !bytes.Equal(s.index[i].key, key) {
		return record{}, false, nil
	}

	if _, err := s.f.Seek(s.index[i].offset, io.SeekStart); err != nil {
		return record{}, false, err
	}

	rec, err := decodeRecord(s.f)
	if err != nil {
		return record{}, false, err
	}

	return rec, true, nil
}

// Close releases the file handle.
func (s *SSTable) Close() error {
	return s.f.Close()
}