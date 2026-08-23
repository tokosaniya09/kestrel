package storage

import "bytes"

// mergeSSTables performs a k-way merge of iters, which are ordered NEWEST FIRST
// (iters[0] is the newest SSTable, iters[len-1] the oldest). It returns the live
// records in ASCENDING KEY ORDER:
//
//   - for each distinct key, the value from the NEWEST iterator that has it;
//   - tombstones dropped entirely — safe because this is a FULL compaction, so
//     no older SSTable survives for a tombstone to shadow.
//
// This is the one function you implement in Phase 3a. See PHASE3.md
// "Step 2 — the merge" for the full walkthrough.
//
// Algorithm (no heap needed; k is small):
//
//	loop:
//	  1. Among all Valid() iterators, find the smallest current key.
//	     If none is valid, you're done — return.
//	  2. The newest version of that key is the FIRST iterator in slice order
//	     (lowest index) whose current key equals the smallest. Remember its record.
//	  3. Advance EVERY iterator whose current key equals the smallest, so each
//	     copy of that key is consumed (each SSTable has a key at most once, so
//	     that's one Next() per matching iterator). Check Err() after advancing.
//	  4. If the remembered (newest) record is NOT a tombstone, append it to output.
//
// You'll want:  import "bytes"   (bytes.Compare for step 1, bytes.Equal for 2-3).
func mergeSSTables(iters []*sstableIterator) ([]record, error) {
	var out []record

	for {
		// 1. Smallest current key across all still-valid iterators.
		var smallest []byte
		for _, it := range iters {
			if !it.Valid() {
				continue
			}
			if smallest == nil || bytes.Compare(it.Record().key, smallest) < 0 {
				smallest = it.Record().key
			}
		}
		if smallest == nil {
			return out, nil // every iterator is exhausted
		}

		// 2 + 3. The newest version is the first (lowest-index) iterator sitting
		// on `smallest`. Remember it, then advance EVERY iterator on that key.
		var live record
		haveLive := false
		for _, it := range iters {
			if it.Valid() && bytes.Equal(it.Record().key, smallest) {
				if !haveLive {
					live = it.Record()
					haveLive = true
				}
				it.Next()
				if it.Err() != nil {
					return nil, it.Err()
				}
			}
		}

		// 4. Keep the newest version unless it's a tombstone.
		if live.kind != opDelete {
			out = append(out, live)
		}
	}
}
