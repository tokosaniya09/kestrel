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
// No heap is needed here: k is the number of SSTables, which stays small, so a
// linear scan for the smallest key each round is cheaper than maintaining one.
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
