package storage

import (
	"fmt"
	"testing"
)

// The defining guarantee: a Bloom filter must NEVER say no to a key it holds.
func TestBloomNoFalseNegatives(t *testing.T) {
	b := newBloom(1000)
	for i := 0; i < 1000; i++ {
		b.add([]byte(fmt.Sprintf("key%d", i)))
	}
	for i := 0; i < 1000; i++ {
		if !b.test([]byte(fmt.Sprintf("key%d", i))) {
			t.Fatalf("false negative for key%d — impossible for a correct Bloom filter", i)
		}
	}
}

// It must actually filter: most absent keys should come back false. With ~10
// bits/key and k=7 the theoretical rate is <1%; allow generous slack.
func TestBloomFiltersMostAbsentKeys(t *testing.T) {
	b := newBloom(1000)
	for i := 0; i < 1000; i++ {
		b.add([]byte(fmt.Sprintf("key%d", i)))
	}
	fp := 0
	for i := 0; i < 1000; i++ {
		if b.test([]byte(fmt.Sprintf("absent%d", i))) {
			fp++
		}
	}
	if fp > 50 {
		t.Fatalf("too many false positives: %d/1000 — the filter isn't filtering", fp)
	}
}

// The filter must survive the round trip through the SSTable file.
func TestBloomEncodeRoundTrip(t *testing.T) {
	b := newBloom(100)
	for i := 0; i < 100; i++ {
		b.add([]byte(fmt.Sprintf("k%d", i)))
	}
	b2 := decodeBloom(b.encode())
	for i := 0; i < 100; i++ {
		if !b2.test([]byte(fmt.Sprintf("k%d", i))) {
			t.Fatalf("k%d lost across encode/decode", i)
		}
	}
}
