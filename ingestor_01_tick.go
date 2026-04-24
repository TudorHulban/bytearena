package bytearena

// tickThreshold performs one consumer iteration:
// - checks if active arena should be sealed due to data above threshold
// - rotates if needed
// - drains writers
// - flushes sealed arena
func (ing *Ingestor) tickThreshold() {
	activeArena := ing.active.Load()
	if activeArena == nil {
		return
	}

	// sealing triggers:
	// Trigger A: rollback pressure (many failed reservations)
	// Trigger B: any subregion crossed its thresold
	if activeArena.rollbackCounter.Load() == 0 {
		for ix := range ing.subRegions {
			if activeArena.subRegionCursors[ix].value.Load() > ing.arenaSealThresholds[ix] {
				goto seal
			}
		}

		return
	}

seal:
	sealedArena := ing.rotate()

	if sealedArena == nil {
		return
	}

	// Wait for writers with timeout + adaptive backoff
	if errWait := ing.waitForWriters(sealedArena); errWait != nil {
		ing.Registry.Inc(TErrDroppedSealedData) // ⚠️ Data in sealedArena is LOST, log and skip flush to avoid hang.

		sealedArena.reset()

		return
	}

	// ✅ Safe to read: all writers finished
	ing.flusher(sealedArena)

	sealedArena.reset()
}

func (ing *Ingestor) tickIfData() {
	activeArena := ing.active.Load()
	if activeArena == nil {
		return
	}

	// sealing triggers:
	// Trigger: any subregion has data
	for ix := range ing.subRegions {
		if activeArena.subRegionCursors[ix].value.Load() > ing.subRegions[ix].Lower {
			goto seal
		}
	}

	return

seal:
	sealedArena := ing.rotate()

	if sealedArena == nil {
		return
	}

	if errWait := ing.waitForWriters(sealedArena); errWait != nil {
		ing.Registry.Inc(TErrDroppedSealedData)

		sealedArena.reset()

		return
	}

	ing.flusher(sealedArena)

	sealedArena.reset()
}
