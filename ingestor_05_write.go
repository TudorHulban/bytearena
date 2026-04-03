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
	return m.beginWrite(n)
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
//   - ok == false
func (m *Ingestor) beginWrite(n uint32) (WriteRegion, error) {
	// Permanently oversized: message can never fit any arena, don't retry.
	// Check this before Enter() to avoid a spurious rollback increment.
	if int32(n) > int32(m.arenaSize) { //nolint:gosec
		return WriteRegion{},
			ErrWriteMessageTooLarge
	}

	arena := m.active.Load()
	if arena == nil {
		return WriteRegion{},
			ErrWriteNoActiveArena
	}

	// Enter BEFORE reserving, but validate we are still on the active arena.
	arena.Enter()

	if m.active.Load() != arena {
		arena.Leave()

		return WriteRegion{},
			ErrWriteActiveArenaMismatch
	}

	// === CAS-based overflow-safe reservation ===
	var offset uint32

	limit := int32(m.arenaSize) - int32(n) //nolint:gosec

	for {
		cur := arena.cursor.Load()

		// Overflow-safe check: avoid computing cur + n directly.
		// At this point n <= arenaSize is guaranteed, so limit >= 0.
		// This branch means the arena is currently too full — signal
		// a flush and let TryWrite retry after rotation.
		if cur > limit {
			arena.AddRollback()

			arena.Leave()
			m.signalFlush()

			return WriteRegion{},
				ErrWriteArenaFull
		}

		next := cur + int32(n) //nolint:gosec

		// Attempt to reserve [cur, next)
		if arena.cursor.CompareAndSwap(cur, next) {
			offset = uint32(cur) //nolint:gosec

			break
		}

		// CAS failed: retry
	}

	// Success
	return WriteRegion{
			arena:  arena,
			offset: offset,
			size:   n,
		},
		nil
}

// write attempts to write n bytes into the active arena.
// The caller provides a function that writes into the reserved buffer.
//
// The write function receives a byte slice of length n and must fill it.
func (m *Ingestor) write(n uint32, fn func(destination []byte)) error {
	region, errWrite := m.TryWrite(n)

	// If the arena was full, wait for the consumer to rotate, then retry once.
	if errWrite == ErrWriteArenaFull { //nolint:errorlint
		staleArena := m.active.Load()

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

		region, errWrite = m.beginWrite(n)
	}

	if errWrite != nil {
		return errWrite
	}

	// Mark write complete.
	defer m.EndWrite(region)

	// Write into the reserved region.
	fn(region.Buf())

	return nil
}
