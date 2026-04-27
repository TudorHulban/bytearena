package bytearena

// rotate seals the current active arena and switches to the other one.
// It returns the sealed arena (the one that was active before the switch).
//
// This function does NOT wait for writers to drain and does NOT flush.
// Waiting for writers and flushing are handled by the consumer logic.
func (ing *Ingestor) rotate() *arena {
	activeArena := ing.active.Load()
	if activeArena == nil {
		return nil
	}

	// Determine the next arena.
	var next *arena
	if activeArena == ing.arenaFirst {
		next = ing.arenaSecond
	} else {
		next = ing.arenaFirst
	}

	// Switch active to the next arena.
	// Switching first avoids weird states where something
	// is both visible as active and sealed.
	ing.active.Store(next)

	return activeArena
}
