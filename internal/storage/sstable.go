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

// SSTable is an immutable, sorted key-value file on disk.
//
// File layout (integers big-endian) — CHANGED in Phase 3b: a FILTER section was
// added and the footer grew from 20 to 36 bytes.
//
//	DATA    : records in ascending key order, each = encodeRecord:
//	          [kind:1][keyLen:4][key][valLen:4][value]
//	INDEX   : one entry per key, ascending: [keyLen:4][key][offset:8]
//	FILTER  : the Bloom filter, [k:4][m:4][bits...]           (NEW)
//	FOOTER  : fixed 36 bytes at the very end:
//	          [indexOffset:8][indexLen:8][filterOffset:8][filterLen:8][magic:4]
//
// NOTE: because the footer size changed, SSTables written before Phase 3b can't
// be read by this code. Delete the ./data directory (or recompact) once.
const (
	sstMagic      uint32 = 0x4B455354 // "KEST"
	sstFooterSize        = 36
)

type indexEntry struct {
	key    []byte
	offset int64
}

type SSTable struct {
	f      *os.File
	index  []indexEntry // ascending by key
	filter *bloomFilter // Phase 3b: skip this file when it says "definitely not"
}

func writeSSTable(path string, m *Memtable) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)

	// DATA section, remembering each record's byte offset for the index.
	var index []indexEntry
	var offset int64
	var writeErr error
	m.ForEach(func(r record) {
		if writeErr != nil {
			return
		}
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
		f.Close()
		return writeErr
	}

	// INDEX section.
	indexOffset := offset
	var indexLen int64
	for _, e := range index {
		if err := binary.Write(w, binary.BigEndian, uint32(len(e.key))); err != nil {
			f.Close()
			return err
		}
		if _, err := w.Write(e.key); err != nil {
			f.Close()
			return err
		}
		if err := binary.Write(w, binary.BigEndian, e.offset); err != nil {
			f.Close()
			return err
		}
		indexLen += int64(4 + len(e.key) + 8)
	}

	// FILTER section (Phase 3b): build a Bloom filter over every key.
	filter := newBloom(len(index))
	for _, e := range index {
		filter.add(e.key)
	}
	fbytes := filter.encode()
	filterOffset := indexOffset + indexLen
	if _, err := w.Write(fbytes); err != nil {
		f.Close()
		return err
	}
	filterLen := int64(len(fbytes))

	// FOOTER (36 bytes).
	for _, v := range []int64{indexOffset, indexLen, filterOffset, filterLen} {
		if err := binary.Write(w, binary.BigEndian, v); err != nil {
			f.Close()
			return err
		}
	}
	if err := binary.Write(w, binary.BigEndian, sstMagic); err != nil {
		f.Close()
		return err
	}

	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

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

	footer := make([]byte, sstFooterSize)
	if _, err := f.ReadAt(footer, fi.Size()-sstFooterSize); err != nil {
		f.Close()
		return nil, err
	}
	indexOffset := int64(binary.BigEndian.Uint64(footer[0:8]))
	indexLen := int64(binary.BigEndian.Uint64(footer[8:16]))
	filterOffset := int64(binary.BigEndian.Uint64(footer[16:24]))
	filterLen := int64(binary.BigEndian.Uint64(footer[24:32]))
	magic := binary.BigEndian.Uint32(footer[32:36])
	if magic != sstMagic {
		f.Close()
		return nil, fmt.Errorf("bad sstable magic in %s (old format? delete ./data)", path)
	}

	// INDEX.
	raw := make([]byte, indexLen)
	if _, err := f.ReadAt(raw, indexOffset); err != nil {
		f.Close()
		return nil, err
	}
	var index []indexEntry
	for len(raw) > 0 {
		klen := binary.BigEndian.Uint32(raw[0:4])
		raw = raw[4:]
		key := append([]byte(nil), raw[:klen]...)
		raw = raw[klen:]
		off := int64(binary.BigEndian.Uint64(raw[0:8]))
		raw = raw[8:]
		index = append(index, indexEntry{key: key, offset: off})
	}

	// FILTER.
	fbuf := make([]byte, filterLen)
	if _, err := f.ReadAt(fbuf, filterOffset); err != nil {
		f.Close()
		return nil, err
	}
	filter := decodeBloom(fbuf)

	return &SSTable{f: f, index: index, filter: filter}, nil
}

func (s *SSTable) Get(key []byte) (record, bool, error) {
	// Phase 3b: consult the Bloom filter first. If it says "definitely not",
	// skip the index search and the disk seek entirely.
	if s.filter != nil && !s.filter.test(key) {
		return record{}, false, nil
	}

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

func (s *SSTable) Close() error {
	return s.f.Close()
}
