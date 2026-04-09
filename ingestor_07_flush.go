package bytearena

import (
	"context"
	"time"
)

// Flushers for sealed arena contents using the provided writer function.
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

// Copy regions and then one write.
func (m *Ingestor) flushArenaIsolatedBuffer(a *arena) {
	if a == nil {
		return
	}

	// Pre-calculate total used bytes across all sub-regions
	var totalUsed uint32

	for ix := range m.subRegions {
		cursorVal := a.subRegionCursors[ix].value.Load()
		lower := m.subRegions[ix].Lower

		// Clamp cursor to region bounds
		if cursorVal < lower {
			cursorVal = lower
		}

		used := cursorVal - lower
		totalUsed += used
	}

	if totalUsed == 0 {
		return
	}

	m.flushScratch = m.flushScratch[:0]
	if cap(m.flushScratch) < int(totalUsed) {
		m.flushScratch = make([]byte, 0, totalUsed)
	}

	// Copy each sub-region's written slice in order.
	for ix := range m.subRegions {
		cursorVal := a.subRegionCursors[ix].value.Load()
		lower := m.subRegions[ix].Lower
		upper := m.subRegions[ix].Upper

		// Clamp and compute written range
		start := lower

		end := cursorVal
		if end < start {
			end = start
		}

		if end > upper {
			end = upper
		}

		if start < end {
			m.flushScratch = append(m.flushScratch, a.buf[start:end]...)
		}
	}

	for len(m.flushScratch) > 0 {
		bytesWritten, errWrite := m.writer.Write(m.flushScratch)
		if errWrite != nil {
			m.Registry.loadError(errWrite)

			return
		}

		if bytesWritten == 0 {
			m.Registry.loadError(ErrWriterNoProgress)

			return
		}

		m.flushScratch = m.flushScratch[bytesWritten:]
	}
}

// Multi-write.
//
// This is default flusher.
func (m *Ingestor) flushArenaPerRegion(a *arena) {
	if a == nil {
		return
	}

	for ix := range m.subRegions {
		lower := m.subRegions[ix].Lower
		upper := m.subRegions[ix].Upper

		end := a.subRegionCursors[ix].value.Load()
		if end < lower {
			end = lower
		}

		if end > upper {
			end = upper
		}

		data := a.buf[lower:end]

		for len(data) > 0 {
			bytesWritten, errWrite := m.writer.Write(data)
			if errWrite != nil {
				m.Registry.loadError(errWrite)

				return
			}

			if bytesWritten == 0 {
				m.Registry.loadError(ErrWriterNoProgress)

				return
			}

			data = data[bytesWritten:]
		}
	}
}

// flushOnShutdown flushes both arenas best-effort.
func (m *Ingestor) flushOnShutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	// First rotation: seal whatever is currently active (call it A).
	firstSealed := m.rotate()

	// Second rotation: seal the other arena (B) which just became active.
	// Any producer that got bumped from A by the first rotate and retried
	// into B will be captured here.
	secondSealed := m.rotate()

	// After two rotates the pointer arithmetic wraps: m.active now points back
	// at firstSealed (only two arenas exist). Without intervention, producers
	// that are still alive can Enter() firstSealed — it looks active — and
	// write into its buf while we are reading it below. That is a data race.
	//
	// Setting active to nil closes the door permanently:
	//   - beginWrite loads nil → returns ErrWriteNoActiveArena immediately.
	//   - Producers already past the nil check but not yet past the active
	//     mismatch check will fail the check (nil != arena) and Leave().
	//   - Producers already past both checks (CAS succeeded) are already
	//     counted in numberWriters; waitForWritersCtx drains them.
	//
	// The spin-wait in write() guards against a nil staleArena before
	// dereferencing it (see ingestor_05_write.go).
	m.active.Store(nil)

	// Flush second-sealed first (it became active most recently,
	// producers who retried land here — wait for them first).
	if secondSealed != nil {
		if errWriteSecond := m.waitForWritersCtx(
			ctx,
			secondSealed,
		); errWriteSecond == nil {
			m.flusher(secondSealed)
		} else {
			m.Registry.Inc(TErrDroppedSealedData)
		}
	}

	// Flush first-sealed.
	if firstSealed != nil && firstSealed != secondSealed {
		if errWriteFirst := m.waitForWritersCtx(
			ctx,
			firstSealed,
		); errWriteFirst == nil {
			m.flusher(firstSealed)
		} else {
			m.Registry.Inc(TErrDroppedSealedData)
		}
	}
}

func (m *Ingestor) signalFlush() {
	select {
	case m.chFlush <- struct{}{}:

	default: // signal already pending, consumer will handle it
	}
}
