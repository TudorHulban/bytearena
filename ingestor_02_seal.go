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
	if a.rollbackCounter.Load() > 0 {
		return true
	}

	for ix, subregion := range m.subRegions {
		regionSize := subregion.Upper - subregion.Lower
		// Per-region seal threshold: absolute offset within arena buffer
		threshold := subregion.Lower + (regionSize * m.arenaSealPercentage / 100) // TODO: precompute

		cursor := a.subRegionCursors[ix].Load()
		if cursor >= threshold {
			return true
		}
	}

	return false
}
