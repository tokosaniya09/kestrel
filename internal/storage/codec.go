package storage

import (
	"encoding/binary"
	"io"
)

// encodeRecord writes r to w in the shared on-disk format and returns the number
// of bytes written:
//
//	[kind:1][keyLen:uint32][key][valLen:uint32][value]
//
// The same format is used by the WAL and by an SSTable's DATA section, so both
// the log and the sorted files speak one wire format. Returning the byte count
// is what lets the SSTable writer track each record's offset.
func encodeRecord(w io.Writer, r record) (int, error) {
	total := 0

	n, err := w.Write([]byte{byte(r.kind)})
	total += n
	if err != nil {
		return total, err
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(r.key))); err != nil {
		return total, err
	}
	total += 4

	n, err = w.Write(r.key)
	total += n
	if err != nil {
		return total, err
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(r.value))); err != nil {
		return total, err
	}
	total += 4

	n, err = w.Write(r.value)
	total += n
	if err != nil {
		return total, err
	}

	return total, nil
}

// decodeRecord reads one record written by encodeRecord. At a clean end of input
// the very first read returns io.EOF, which callers (WAL replay) use to stop
// looping. A truncated record instead yields io.ErrUnexpectedEOF.
func decodeRecord(r io.Reader) (record, error) {
	var kind [1]byte
	if _, err := io.ReadFull(r, kind[:]); err != nil {
		return record{}, err
	}

	var keyLen uint32
	if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
		return record{}, err
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return record{}, err
	}

	var valLen uint32
	if err := binary.Read(r, binary.BigEndian, &valLen); err != nil {
		return record{}, err
	}
	val := make([]byte, valLen)
	if _, err := io.ReadFull(r, val); err != nil {
		return record{}, err
	}

	return record{kind: op(kind[0]), key: key, value: val}, nil
}
