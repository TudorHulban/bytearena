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

		// Check bounds: cur must be within [lower, limit] to reserve [cur, cur+n)
		if cur < lower || cur > limit {
			return 0,
				ErrWriteSubRegionFull
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
