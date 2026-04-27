package bytearena

import (
	"sync/atomic"
)

// reserveBytes attempts to atomically reserve n bytes within [lower, upper)
// using the provided cursor. Returns the reserved offset or an error.
// The caller must ensure lower/upper are valid and n <= (upper - lower).
func (*Ingestor) reserveBytes(cursor *atomic.Uint32, toReserve uint32, lower, upper uint32) (uint32, error) {
	limit := upper - toReserve // safe: caller ensures n <= upper-lower

	for {
		cur := cursor.Load()

		// The cursor is initialized to Lower in resetSubRegions
		// and only ever moves forward via CAS (adding positive values).
		// It cannot go below lower after initialization,
		// and waitForWriters guarantees no writers are active before reset runs.
		// This is why below does not check cur < lower.

		// Check bounds: cur must be within [lower, limit] to reserve [cur, cur+n)
		if cur > limit {
			return 0,
				errWriteSubRegionFull
		}

		next := cur + toReserve //nolint:gosec

		// TODO: debug only
		// m.Metrics.NumberCAS.Add(1)

		if cursor.CompareAndSwap(cur, next) {
			return cur, nil //nolint:gosec
		}

		// CAS failed: retry in next iteration (after pause).
		// helpers.Pause(1)
	}
}
