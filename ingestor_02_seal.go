package bytearena

// shouldSeal determines whether the active arena should be sealed.
//
// This is a simple heuristic combining:
//   - cursor threshold (almost full)
//   - rollback pressure (many failed reservations)
//
// The exact thresholds can be tuned later.
func (ing *Ingestor) shouldSeal(a *arena) bool {
	if a.rollbackCounter.Load() > 0 {
		return true
	}

	for ix, threshold := range ing.arenaSealThresholds {
		cursor := a.subRegionCursors[ix].value.Load()
		if cursor >= threshold || cursor > ing.subRegions[ix].Lower {
			return true
		}
	}

	return false
}
