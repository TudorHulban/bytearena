package bytearena

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
		if cur > limit {
			arena.AddRollback()
			arena.Leave()
			m.signalFlush()

			return WriteRegion{}, ErrWriteMessageTooLarge
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
	}, nil
}

// write attempts to write n bytes into the active arena.
// The caller provides a function that writes into the reserved buffer.
//
// The write function receives a byte slice of length n and must fill it.
func (m *Ingestor) write(n uint32, fn func(destination []byte)) error {
	// Try to region space (with one retry).
	region, canWrite := m.TryWrite(n)
	if canWrite != nil {
		return canWrite
	}

	// Mark write complete.
	defer m.EndWrite(region)

	// Write into the reserved region.
	fn(region.Buf())

	return nil
}
