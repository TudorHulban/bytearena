package bytearena

// shouldSeal determines whether the active arena should be sealed.
//
// This is a simple heuristic combining:
//   - cursor threshold (almost full)
//   - rollback pressure (many failed reservations)
//
// The exact thresholds can be tuned later.
func (m *Ingestor) shouldSeal(a *arena) bool {
	// Rollback pressure: many producers failed to reserve space.
	// This indicates high contention near the end of the arena.
	if a.rollbackCounter.Load() > 0 {
		return true
	}

	return a.cursor.Load() >= m.arenaSealThreshold
}
