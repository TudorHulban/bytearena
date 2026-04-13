package bytearena

import (
	"runtime"
)

// TryWrite attempts beginWrite once. If it fails, it reloads the active
// arena and tries exactly one more time.
//
// This is a convenience helper for callers who want a simple
// "try once, rotate may have happened, try again" pattern.
//
// It does NOT loop indefinitely and does NOT block.
func (ing *Ingestor) TryWrite(n uint32) (writeRegion, error) {
	// First attempt.
	region, errWrite := ing.beginWrite(n)
	if errWrite == nil {
		return region, nil
	}

	// Do not retry permanently oversized messages.
	// errors.Is is too slow.
	if errWrite == ErrWriteMessageTooLarge { //nolint:errorlint
		ing.Registry.Inc(TErrWriteMessageTooLarge)

		return writeRegion{}, errWrite
	}

	ing.Registry.loadError(errWrite)

	// Reload active arena — rotation may have occurred.
	// Second attempt.
	region, errWrite = ing.beginWrite(n)
	if errWrite != nil {
		ing.Registry.loadError(errWrite)

		return writeRegion{}, errWrite
	}

	return region, errWrite
}

// beginWrite attempts to reserve n bytes in the current active arena.
//
// On success:
//   - writers-in-flight is incremented
//   - a region is returned
//   - caller MUST call EndWrite
//
// On failure:
//   - writers-in-flight is decremented
//   - reservation if reversed
//   - rollback counter is incremented
func (ing *Ingestor) beginWrite(toReserve uint32) (writeRegion, error) {
	if toReserve > ing.maxMessageSize {
		return writeRegion{},
			ErrWriteMessageTooLarge
	}

	arena := ing.active.Load()
	if arena == nil {
		return writeRegion{},
			ErrWriteNoActiveArena
	}

	arena.Enter()

	if ing.active.Load() != arena {
		arena.Leave()

		return writeRegion{},
			ErrWriteActiveArenaMismatch
	}

	// Round-robin: select sub-region using request counter (bit-mask for power-of-2)
	regionIdx := ing.counterRequests.Add(1) & 7
	subRegion := ing.subRegions[regionIdx]

	// Fast-fail if message does not fit this sub-region
	if toReserve > (subRegion.Upper - subRegion.Lower) {
		arena.AddRollback()
		arena.Leave()
		ing.signalFlush()

		return writeRegion{},
			ErrWriteSubRegionFull
	}

	offset, errReserve := ing.reserveBytes(&arena.subRegionCursors[regionIdx].value, toReserve, subRegion.Lower, subRegion.Upper)
	if errReserve != nil {
		arena.AddRollback()
		arena.Leave()
		ing.signalFlush()

		return writeRegion{},
			errReserve
	}

	// Success: return write handle
	return writeRegion{
			arena:  arena,
			offset: offset,
			size:   toReserve,
		},
		nil
}

// write attempts to write n bytes into the active arena.
// The caller provides a function that writes into the reserved buffer.
//
// The write function receives a byte slice of length n and must fill it.
func (ing *Ingestor) write(n uint32, fn func(destination []byte)) error {
	var (
		region      writeRegion
		errTryWrite error
	)

	// By loading m.active before TryWrite, staleArena is guaranteed to be the arena that is actually full.
	// The consumer must either:
	// a. rotate away from it (m.active != staleArena → spin exits), or
	// b. reset it after flushing (staleArena.epoch bumps → spin exits)
	staleArena := ing.active.Load()

	region, errTryWrite = ing.TryWrite(n)

	// If the arena was full, wait for the consumer to rotate, then retry once.
	if errTryWrite == ErrWriteSubRegionFull { //nolint:errorlint
		// flushOnShutdown sets active to nil as a sentinel after the double-rotate.
		// If we see nil here (or isStopped is already set), bail out immediately.
		// Without this guard, staleArena.epoch.Load() would panic on a nil pointer.
		if staleArena == nil || ing.isStopped.Load() {
			ing.Registry.Inc(TErrWriteShuttingDown)

			return ErrWriteShuttingDown
		}

		staleEpoch := staleArena.epoch.Load()

		// Spin until the consumer has swapped in a fresh arena.
		// When the consumer recycles arena A (after the double rotation), reset() bumps stale.epoch,
		// the second condition goes false,
		// the goroutine exits the loop and calls beginWrite on the fresh arena with no deadlock.
		for ing.active.Load() == staleArena && staleArena.epoch.Load() == staleEpoch {
			if ing.isStopped.Load() { // ← consumer is gone, bail out
				ing.Registry.Inc(TErrWriteShuttingDown)

				return ErrWriteShuttingDown
			}

			runtime.Gosched()
		}

		var errWrite error

		region, errWrite = ing.beginWrite(n)
		if errWrite != nil {
			return errWrite
		}

		// Retry succeeded: clear errTryWrite so the check below does not
		// return ErrWriteArenaFull while holding an unreleased numberWriters.
		errTryWrite = nil
	}

	if errTryWrite != nil {
		return errTryWrite
	}

	// Mark write complete.
	defer ing.EndWrite(region)

	// Write into the reserved region.
	fn(region.Buf())

	return nil
}

// EndWrite decrements writers-in-flight.
//
// EndWrite must be called before the context is cancelled if wait on chIngestionEnd is used.
// TryWrite/beginWrite increments the writers-in-flight counter and flushOnShutdown will spin on it indefinitely.
// Using defer for EndWrite is only safe when the caller is not also waiting for ingestion to drain.
func (*Ingestor) EndWrite(r writeRegion) {
	r.arena.Leave()
}
