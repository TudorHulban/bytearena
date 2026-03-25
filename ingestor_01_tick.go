package bytearena

// tick performs one consumer iteration:
// - checks if active arena should be sealed
// - rotates if needed
// - drains writers
// - flushes sealed arena
func (m *Ingestor) tick(flusher flusher) {
	activeArena := m.active.Load()
	if activeArena == nil {
		return
	}

	if !m.shouldSeal(activeArena) {
		return
	}

	sealedArena := m.rotate()
	if sealedArena == nil {
		return
	}

	m.waitForWriters(sealedArena)

	used := min(sealedArena.cursor.Load(), int32(m.arenaSize)) //nolint:gosec
	if used > 0 {
		flusher(sealedArena)
	}

	sealedArena.reset()
	m.sealed.Store(nil)
}
