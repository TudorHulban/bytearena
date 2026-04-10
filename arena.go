package bytearena

// arena.go

import (
	"sync/atomic"
)

// note:
// If two atomic.Uint32 values are <64 bytes apart
// in memory → cache-line contention → performance degradation

// paddedCursor wraps an atomic.Uint32 with cache-line padding
type paddedCursor struct {
	value atomic.Uint32
	_     [60]byte // pad to 64 bytes (4 + 60 = 64)
}

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
	subRegions [8]subRegion

	// Per-subregion CAS cursors: one atomic counter per shard
	subRegionCursors [8]paddedCursor
}

func newArena(arenaSize uint32, subRegions [8]subRegion) *arena {
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
	for ix := range a.subRegionCursors {
		// Restore to the sub-region's lower bound, NOT zero.
		// PaddedCursor.value is embedded and pre-allocated with the arena;
		// no nil checks or allocations needed.
		a.subRegionCursors[ix].value.Store(a.subRegions[ix].Lower)
	}
}

func (a *arena) getCursorValues() []uint32 {
	result := make([]uint32, len(a.subRegionCursors))

	for ix := range len(a.subRegionCursors) {
		result[ix] = a.subRegionCursors[ix].value.Load()
	}

	return result
}

func (a *arena) getSubregionLoads() ([]uint32, uint32) {
	result := make([]uint32, len(a.subRegionCursors))

	var total uint32

	for ix := range len(a.subRegionCursors) {
		result[ix] = a.subRegionCursors[ix].value.Load() - a.subRegions[ix].Lower

		total = total + result[ix]
	}

	return result, total
}
