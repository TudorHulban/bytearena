package bytearena

import "sync/atomic"

// reserveBytes attempts to atomically reserve n bytes within [lower, upper)
// using the provided cursor. Returns the reserved offset or an error.
// The caller must ensure lower/upper are valid and n <= (upper - lower).
func reserveBytes(cursor *atomic.Uint32, n uint32, lower, upper uint32) (uint32, error) {
	limit := uint32(upper - n) // safe: caller ensures n <= upper-lower

	for {
		cur := cursor.Load()

		// Check bounds: cur must be within [lower, limit] to reserve [cur, cur+n)
		if cur < uint32(lower) || cur > limit {
			return 0, ErrWriteArenaFull // or a more specific "subregion full" error
		}

		next := cur + uint32(n) //nolint:gosec

		if cursor.CompareAndSwap(cur, next) {
			return uint32(cur), nil //nolint:gosec
		}
		// CAS failed: retry
	}
}
