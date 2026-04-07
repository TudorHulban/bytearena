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
func (m *Ingestor) tick() {
	activeArena := m.active.Load()
	if activeArena == nil || !m.shouldSeal(activeArena) {
		return
	}

	sealedArena := m.rotate()
	if sealedArena == nil {
		return
	}

	// ✅ Create transient context with configurable timeout
	ctxUnblockWriterWait, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(m.milisecondsUnblock)*time.Millisecond,
	)
	defer cancel() // Always clean up resources

	// ✅ Wait for writers with timeout + adaptive backoff
	if errWait := m.waitForWritersCtx(ctxUnblockWriterWait, sealedArena); errWait != nil {
		m.Registry.Inc(TErrDroppedSealedData) // ⚠️ Data in sealedArena is LOST, log and skip flush to avoid hang.

		sealedArena.reset()

		return
	}

	// ✅ Safe to read: all writers finished
	m.flusher(sealedArena)

	sealedArena.reset()
}
