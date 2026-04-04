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
	epoch atomic.Uint64
	_     [56]byte // pad to 64 bytes

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

	telemetryObservableRollback func(add uint64)

	// subRegions holds the Lower/Upper bounds for each shard.
	// Stored here so reset can restore cursors to their correct Lower values
	// without the Ingestor passing them in on every call.
	subRegions [8]SubRegion

	// Per-subregion CAS cursors: one atomic counter per shard
	subRegionCursors [8]*atomic.Uint32
}

func newArena(arenaSize uint32, subRegions [8]SubRegion) *arena {
	result := arena{
		buf:        make([]byte, arenaSize),
		subRegions: subRegions,
	}

	result.resetSubRegions()

	return &result
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
	a.resetSubRegions()

	// numberWriters is intentionally NOT reset here.
	// waitForWriters guarantees it reaches zero before this arena
	// is reused. Resetting it here would race with in-flight writers
	// still holding Enter(), corrupting the count to -1 and hanging
	// the next waitForWriters call permanently.

	if a.telemetryObservableRollback != nil {
		a.telemetryObservableRollback(
			uint64(a.rollbackCounter.Swap(0)), //nolint:gosec
		)

		a.epoch.Add(1)

		return
	}

	a.rollbackCounter.Store(0)
	a.epoch.Add(1)
}

func (a *arena) resetSubRegions() {
	for ix := range len(a.subRegionCursors) {
		if a.subRegionCursors[ix] == nil {
			a.subRegionCursors[ix] = new(atomic.Uint32)
		}
		// Restore to the sub-region's lower bound, NOT zero.
		// Allocating new(atomic.Uint32) starts at 0, which is below Lower for
		// regions 1-7 and causes reserveBytes to return ErrWriteArenaFull immediately.
		a.subRegionCursors[ix].Store(a.subRegions[ix].Lower)
	}
}
func (a *arena) getCursorValues() []uint64 {
	result := make([]uint64, len(a.subRegionCursors))

	for i := 0; i < len(a.subRegionCursors); i++ {
		cur := a.subRegionCursors[i]
		if cur == nil {
			result[i] = 0
			continue
		}
		result[i] = uint64(cur.Load())
	}

	return result
}
