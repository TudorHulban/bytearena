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
	var (
		region        writeRegion
		errWriteFirst error
	)

	// First attempt.
	region, errWriteFirst = ing.fnBeginWrite(ing, n)
	if errWriteFirst == nil {
		return region, nil
	}

	// Do not retry permanently oversized messages.
	// errors.Is is too slow.
	if errWriteFirst == errWriteMessageTooLarge { //nolint:errorlint
		ing.Registry.Inc(TErrWriteMessageTooLarge)

		return writeRegion{}, errWriteFirst
	}

	var errWriteSecond error

	// Reload active arena — rotation may have occurred.
	// Second attempt,
	// also loads to error registry if error as final failure.
	region, errWriteSecond = ing.fnBeginWrite(ing, n)
	if errWriteSecond != nil {
		ing.Registry.loadError(errWriteSecond)

		return writeRegion{}, errWriteSecond
	}

	return region, nil
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
	if errTryWrite == errWriteSubRegionFull { //nolint:errorlint
		// flushOnShutdown sets active to nil as a sentinel after the double-rotate.
		// If we see nil here (or isStopped is already set), bail out immediately.
		// Without this guard, staleArena.epoch.Load() would panic on a nil pointer.
		if staleArena == nil { // || ing.isStopped.Load()
			ing.Registry.Inc(TErrWriteShuttingDown)

			return errWriteShuttingDown
		}

		staleEpoch := staleArena.epoch.Load()

		// Spin until the consumer has swapped in a fresh arena.
		// When the consumer recycles arena A (after the double rotation), reset() bumps stale.epoch,
		// the second condition goes false,
		// the goroutine exits the loop and calls beginWrite on the fresh arena with no deadlock.
		// No isStopped check needed: nil active already breaks the condition.
		for ing.active.Load() == staleArena && staleArena.epoch.Load() == staleEpoch {
			runtime.Gosched()
		}

		var errWrite error

		region, errWrite = ing.fnBeginWrite(ing, n)
		if errWrite != nil {
			// active is nil → this is a shutdown, reclassify the error
			if ing.active.Load() == nil {
				ing.Registry.Inc(TErrWriteShuttingDown)

				return errWriteShuttingDown
			}

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
