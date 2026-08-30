package storage

import (
	"encoding/binary"
	"hash/fnv"
)

// A Bloom filter is a bit array + k hash functions that answers "might this key
// be in the set?" It never says no to a key it holds (no false negatives), but
// occasionally says yes to one it doesn't (a false positive). We keep one per
// SSTable so a Get can skip files that provably don't contain the key.
//
// Sizing rule of thumb: ~10 bits per key with k=7 hashes gives a false-positive
// rate under 1%. More bits per key -> fewer false positives, bigger filter.
const (
	bloomBitsPerKey = 10
	bloomNumHashes  = 7
)

type bloomFilter struct {
	bits []byte
	k    uint32 // number of hash functions
	m    uint32 // total number of bits (== len(bits) * 8)
}

// ---------------------------------------------------------------------------
// PROVIDED helpers — you call these; you don't need to change them.
// ---------------------------------------------------------------------------

// hashes derives TWO 32-bit hashes from a single FNV-1a hash of key. The k bit
// positions are then generated as h1 + i*h2 (the Kirsch–Mitzenmacher trick),
// which behaves like k independent hashes for far less work. You'll use this in
// add and test.
func hashes(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(key)
	sum := h.Sum64()
	h1 := uint32(sum)       // low 32 bits
	h2 := uint32(sum >> 32) // high 32 bits
	if h2 == 0 {
		h2 = 1 // a zero step would make every g_i equal — avoid it
	}
	return h1, h2
}

// encode serializes the filter as [k:4][m:4][bits...] for storage in the SSTable.
func (b *bloomFilter) encode() []byte {
	out := make([]byte, 8+len(b.bits))
	binary.BigEndian.PutUint32(out[0:4], b.k)
	binary.BigEndian.PutUint32(out[4:8], b.m)
	copy(out[8:], b.bits)
	return out
}

// decodeBloom parses what encode produced.
func decodeBloom(raw []byte) *bloomFilter {
	k := binary.BigEndian.Uint32(raw[0:4])
	m := binary.BigEndian.Uint32(raw[4:8])
	bits := make([]byte, len(raw)-8)
	copy(bits, raw[8:])
	return &bloomFilter{bits: bits, k: k, m: m}
}

// newBloom returns a filter sized for n expected keys (bloomBitsPerKey bits each,
// bloomNumHashes hashes). Round the bit count up to a whole number of bytes, and
// keep a sensible floor so tiny filters still work.
func newBloom(n int) *bloomFilter {
	if n < 1 {
		n = 1
	}

	m := uint32(n * bloomBitsPerKey)
	if m < 64 {
		m = 64 // floor so tiny SSTables still get a usable filter
	}

	m = (m + 7) &^ 7 // round UP to a multiple of 8 bits

	return &bloomFilter{
		bits: make([]byte, m/8),
		k:    bloomNumHashes,
		m:    m,
	}
}

// add records key in the filter: set the k bits at positions h1 + i*h2 (mod m).
func (b *bloomFilter) add(key []byte) {
	h1, h2 := hashes(key)

	for i := uint32(0); i < b.k; i++ {
		pos := uint32((uint64(h1) + uint64(i)*uint64(h2)) % uint64(b.m))
		b.bits[pos/8] |= 1 << (pos % 8)
	}
}

// test reports whether key MIGHT be present. Return false the instant any of the
// k bits is unset (definitely absent); true only if all k are set.
func (b *bloomFilter) test(key []byte) bool {
	h1, h2 := hashes(key)

	for i := uint32(0); i < b.k; i++ {
		pos := uint32((uint64(h1) + uint64(i)*uint64(h2)) % uint64(b.m))

		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}

	return true
}
