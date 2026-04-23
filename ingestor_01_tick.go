package bytearena

import (
	"context"
	"time"
)

// tick performs one consumer iteration:
// - checks if active arena should be sealed
// - rotates if needed
// - drains writers
// - flushes sealed arena
func (ing *Ingestor) tick() {
	activeArena := ing.active.Load()
	if activeArena == nil {
		return
	}

	// sealing triggers:
	// Trigger A: rollback pressure (many failed reservations)
	// Trigger B: any subregion crossed its lower bound
	if activeArena.rollbackCounter.Load() == 0 {
		for ix := range ing.subRegions {
			if activeArena.subRegionCursors[ix].value.Load() > ing.subRegions[ix].Lower {
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

	// ✅ Create transient context with configurable timeout
	ctxUnblockWriterWait, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(ing.millisecondsUnblock)*time.Millisecond,
	)
	defer cancel() // Always clean up resources

	// ✅ Wait for writers with timeout + adaptive backoff
	if errWait := ing.waitForWritersCtx(ctxUnblockWriterWait, sealedArena); errWait != nil {
		ing.Registry.Inc(TErrDroppedSealedData) // ⚠️ Data in sealedArena is LOST, log and skip flush to avoid hang.

		sealedArena.reset()

		return
	}

	// ✅ Safe to read: all writers finished
	ing.flusher(sealedArena)

	sealedArena.reset()
}
