package bytearena

import "sync/atomic"

// WriteRegion describes a reserved region inside an arena.
type WriteRegion struct {
	arena *arena

	offset uint32
	size   uint32
}

// Buf returns the writable slice for the reserved region.
func (r WriteRegion) Buf() []byte {
	return r.arena.buf[r.offset : r.offset+r.size]
}

// EndWrite decrements writers-in-flight.
//
// EndWrite must be called before the context is cancelled if wait on chIngestionEnd is used.
// TryWrite/beginWrite increments the writers-in-flight counter and flushOnShutdown will spin on it indefinitely.
// Using defer for EndWrite is only safe when the caller is not also waiting for ingestion to drain.
func (*Ingestor) EndWrite(r WriteRegion) {
	r.arena.Leave()
}

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
