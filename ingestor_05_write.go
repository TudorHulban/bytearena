package bytearena

import (
	"runtime"
)

// TryWrite attempts BeginWrite once. If it fails, it reloads the active
// arena and tries exactly one more time.
//
// This is a convenience helper for callers who want a simple
// "try once, rotate may have happened, try again" pattern.
//
// It does NOT loop indefinitely and does NOT block.
func (m *Ingestor) TryWrite(n uint32) (WriteRegion, error) {
	// First attempt.
	region, errWrite := m.beginWrite(n)
	if errWrite == nil {
		return region, nil
	}

	// Do not retry permanently oversized messages.
	// errors.Is is too slow.
	if errWrite == ErrWriteMessageTooLarge { //nolint:errorlint
		m.Registry.Inc(TErrWriteMessageTooLarge)

		return WriteRegion{}, errWrite
	}

	m.Registry.loadError(errWrite)

	// Reload active arena — rotation may have occurred.
	// Second attempt.
	region, errWrite = m.beginWrite(n)
	if errWrite != nil {
		m.Registry.loadError(errWrite)

		return WriteRegion{}, errWrite
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
func (m *Ingestor) beginWrite(toReserve uint32) (WriteRegion, error) {
	if toReserve > m.maxMessageSize {
		return WriteRegion{},
			ErrWriteMessageTooLarge
	}

	arena := m.active.Load()
	if arena == nil {
		return WriteRegion{},
			ErrWriteNoActiveArena
	}

	arena.Enter()

	if m.active.Load() != arena {
		arena.Leave()

		return WriteRegion{},
			ErrWriteActiveArenaMismatch
	}

	// Round-robin: select sub-region using request counter (bit-mask for power-of-2)
	regionIdx := m.counterRequests.Add(1) & 7
	subRegion := m.subRegions[regionIdx]

	// Fast-fail if message doesn't fit this sub-region
	if toReserve > (subRegion.Upper - subRegion.Lower) {
		arena.AddRollback()
		arena.Leave()
		m.signalFlush()

		return WriteRegion{},
			ErrWriteSubRegionFull
	}

	offset, errReserve := m.reserveBytes(&arena.subRegionCursors[regionIdx].value, toReserve, subRegion.Lower, subRegion.Upper)
	if errReserve != nil {
		arena.AddRollback()
		arena.Leave()
		m.signalFlush()

		return WriteRegion{},
			errReserve
	}

	// Success: return write handle
	return WriteRegion{
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
func (m *Ingestor) write(n uint32, fn func(destination []byte)) error {
	var (
		region      WriteRegion
		errTryWrite error
	)

	// By loading m.active before TryWrite, staleArena is guaranteed to be the arena that is actually full.
	// The consumer must either:
	// a. rotate away from it (m.active != staleArena → spin exits), or
	// b. reset it after flushing (staleArena.epoch bumps → spin exits)
	staleArena := m.active.Load()

	region, errTryWrite = m.TryWrite(n)

	// If the arena was full, wait for the consumer to rotate, then retry once.
	if errTryWrite == ErrWriteSubRegionFull { //nolint:errorlint
		// flushOnShutdown sets active to nil as a sentinel after the double-rotate.
		// If we see nil here (or isStopped is already set), bail out immediately.
		// Without this guard, staleArena.epoch.Load() would panic on a nil pointer.
		if staleArena == nil || m.isStopped.Load() {
			m.Registry.Inc(TErrWriteShuttingDown)

			return ErrWriteShuttingDown
		}

		staleEpoch := staleArena.epoch.Load()

		// Spin until the consumer has swapped in a fresh arena.
		// When the consumer recycles arena A (after the double rotation), reset() bumps stale.epoch,
		// the second condition goes false,
		// the goroutine exits the loop and calls beginWrite on the fresh arena with no deadlock.
		for m.active.Load() == staleArena && staleArena.epoch.Load() == staleEpoch {
			if m.isStopped.Load() { // ← consumer is gone, bail out
				m.Registry.Inc(TErrWriteShuttingDown)

				return ErrWriteShuttingDown
			}

			runtime.Gosched()
		}

		var errWrite error

		region, errWrite = m.beginWrite(n)
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
	defer m.EndWrite(region)

	// Write into the reserved region.
	fn(region.Buf())

	return nil
}
