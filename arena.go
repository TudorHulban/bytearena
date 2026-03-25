package bytearena

// arena.go

import (
	"sync/atomic"
)

// arena represents a single fixed-size logging buffer used in a
// double-buffered, lock-free producer/consumer setup.
//
// Methods are defined elsewhere; this file only defines the data layout.
type arena struct { //nolint:govet
	// Hot atomics (each on its own cache line).

	// cursor is the current write position (in bytes) inside buf.
	// Producers use atomic fetch-add on this to reserve regions.
	cursor atomic.Int32
	_      [60]byte // pad to 64 bytes (typical cache line size)

	// numberWriters tracks the number of producers currently writing into this arena.
	// The consumer waits for this to reach zero before flushing.
	numberWriters atomic.Int32
	_             [60]byte // pad to 64 bytes

	// rollbackCounter counts failed reservations near the end of the arena.
	// Used by the consumer as a signal that the arena is under pressure.
	rollbackCounter atomic.Int32
	_               [60]byte // pad to 64 bytes

	// buf is the underlying byte storage for this arena.
	// Its capacity defines the arena size.
	buf []byte
}

// Reserve attempts to reserve N bytes inside the arena.
//
// It returns:
//
//	offset >= 0  → success, producer may write into buf[offset : offset+N]
//	offset < 0   → reservation failed (overflow)
//
// The caller must check (offset + N <= arenaSize).
// If not, the caller must treat this as a failed reservation and NOT write.
//
// This function does NOT roll back the cursor; rollback is logical only.
// The consumer will reset the arena when rotating.
//
// Returns the offset at which the producer is entitled to write.
func (a *arena) Reserve(n uint32) uint32 {
	// Atomically reserve space by bumping the cursor.
	// offset = old cursor value
	return uint32(a.cursor.Add(int32(n))) - n //nolint:gosec
}

// Enter increments the writers-in-flight counter.
// Producers must call this before attempting a reservation.
func (a *arena) Enter() {
	a.numberWriters.Add(1)
}

// Leave decrements the writers-in-flight counter.
// Producers must call this after finishing their write.
func (a *arena) Leave() {
	a.numberWriters.Add(-1)
}

// AddRollback increments the rollback counter.
// Producers call this when a reservation overflows the arena.
func (a *arena) AddRollback() {
	a.rollbackCounter.Add(1)
}

// reset clears the arena state so it can be reused after flushing.
// This does NOT reallocate the buffer.
func (a *arena) reset() {
	a.cursor.Store(0)

	// numberWriters is intentionally NOT reset here.
	// waitForWriters guarantees it reaches zero before this arena
	// is reused. Resetting it here would race with in-flight writers
	// still holding Enter(), corrupting the count to -1 and hanging
	// the next waitForWriters call permanently.

	a.rollbackCounter.Store(0)
}
