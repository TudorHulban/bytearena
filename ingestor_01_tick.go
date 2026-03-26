package bytearena

// tick performs one consumer iteration:
// - checks if active arena should be sealed
// - rotates if needed
// - drains writers
// - flushes sealed arena
func (m *Ingestor) tick() {
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
		m.flusher(sealedArena)
	}

	sealedArena.reset()
	m.sealed.Store(nil)
}
