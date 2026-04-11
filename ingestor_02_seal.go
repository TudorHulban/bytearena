package bytearena

// shouldSeal determines whether the active arena should be sealed.
//
// This is a simple heuristic combining:
//   - cursor threshold (almost full)
//   - rollback pressure (many failed reservations)
//
// The exact thresholds can be tuned later.
func (ing *Ingestor) shouldSeal(a *arena) bool {
	// Rollback pressure: many producers failed to reserve space.
	if a.rollbackCounter.Load() > 0 {
		return true
	}

	for ix, threshold := range ing.arenaSealThresholds {
		if a.subRegionCursors[ix].value.Load() >= threshold {
			return true
		}
	}

	return false
}
