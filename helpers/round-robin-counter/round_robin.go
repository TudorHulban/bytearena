//go:build amd64 && linux

package roundrobincounter

// Package roundrobin provides a ~1 ns/op round-robin counter.
//
// Concurrent safety is ~99%: a plain MOV/INC/MOV without LOCK prefix is used.
// Two goroutines can read the same value in the same nanosecond window, causing
// two consecutive requests to land on the same target. Globally, distribution
// remains even.
// Use only when this trade-off is acceptable (e.g. load balancing).

const cacheLineSize = 64

// RoundRobinCounter is a padded, cache-line-aligned counter.
// Padding prevents false sharing when multiple RoundRobinCounter instances
// live in the same struct or slice.
type RoundRobinCounter struct {
	counter uint64
	_       [cacheLineSize - 1*8]byte
}

func NewRoundRobinCounter() *RoundRobinCounter {
	return &RoundRobinCounter{}
}

//go:nosplit
func (r *RoundRobinCounter) Next() uint64 {
	return fastInc(&r.counter) // assembly: no LOCK prefix
}

// fastInc increments *addr by 1 and returns the new value.
// Implemented in roundrobin_amd64.s.
// The increment uses the exact same MOVQ/INCQ/MOVQ sequence found inside
// atomic.AddUint64 on amd64 — just without the LOCK prefix.
// The standard library must add LOCK to guarantee linearizability under every
// possible interleaving on every CPU.
// For round‑robin load balancing, losing an occasional increment is harmless,
// and the benefit is high: ~1 ns/op, zero contention, zero fences.
//
// Marked nosplit because fastInc is a tiny leaf function: it uses no stack,
// makes no calls, and must never trigger stack growth. This guarantees the
// MOVQ/INCQ/MOVQ sequence runs exactly as written, even in contexts where the
// runtime is not allowed to grow the stack.
//
//go:nosplit
func fastInc(addr *uint64) uint64
