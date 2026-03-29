package bytearena

import "context"

// Flush sealed arena contents using the provided writer function.
//
// The writer receives:
//   - the arena pointer
//   - the slice of bytes to flush
//
// This function does NOT:
//   - rotate arenas
//   - wait for writers
//   - reset the arena
//   - handle errors
//
// Those responsibilities belong to the consumer loop.
func (m *Ingestor) flushArena(a *arena) {
	if a == nil {
		return
	}

	used := a.cursor.Load()
	if used <= 0 {
		return
	}

	if used > int32(m.arenaSize) { //nolint:gosec
		used = int32(m.arenaSize) //nolint:gosec
	}

	buf := a.buf[:used]

	for len(buf) > 0 {
		bytesWritten, errWrite := m.writer.Write(buf)
		if errWrite != nil {
			// Partial writes are allowed even when err != nil.
			// We stop because the caller cannot recover meaningfully.
			return
		}

		if bytesWritten == 0 {
			return // for zero progress writers
		}

		buf = buf[bytesWritten:]
	}
}

// flushOnShutdown flushes both arenas best-effort.
func (m *Ingestor) flushOnShutdown(ctx context.Context) {
	// First rotation: seal whatever is currently active (call it A).
	firstSealed := m.rotate()

	// Second rotation: seal the other arena (B) which just became active.
	// Any producer that got bumped from A by the first rotate and retried
	// into B will be captured here.
	secondSealed := m.rotate()

	// Flush second-sealed first (it became active most recently,
	// producers who retried land here — wait for them first).
	if secondSealed != nil {
		m.waitForWritersCtx(ctx, secondSealed)

		used := secondSealed.cursor.Load()
		if used > 0 {
			m.flusher(secondSealed)
		}
	}

	// Flush first-sealed.
	if firstSealed != nil && firstSealed != secondSealed {
		m.waitForWritersCtx(ctx, firstSealed)

		used := firstSealed.cursor.Load()
		if used > 0 {
			m.flusher(firstSealed)
		}
	}
}

func (m *Ingestor) signalFlush() {
	select {
	case m.chFlush <- struct{}{}:

	default: // signal already pending, consumer will handle it
	}
}
