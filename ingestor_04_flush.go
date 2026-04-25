package bytearena

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
func (ing *Ingestor) flushArenaIsolatedBuffer(a *arena) {
	if a == nil {
		return
	}

	// Cache cursors once to avoid double atomic load per sub-region.
	// [8] matches the round-robin mask (& 7) in beginWrite.
	var cursors [8]uint32

	// Pre-calculate total used bytes across all sub-regions
	var totalUsed uint32

	for ix := range ing.subRegions {
		cursorVal := a.subRegionCursors[ix].value.Load()
		cursors[ix] = cursorVal // ← Cache for second loop (zero alloc, stack-only)
		lower := ing.subRegions[ix].Lower

		// Clamp cursor to region bounds
		if cursorVal < lower {
			cursorVal = lower
		}

		used := cursorVal - lower
		totalUsed = totalUsed + used
	}

	if totalUsed == 0 {
		return
	}

	ing.flushScratch = ing.flushScratch[:0]
	if cap(ing.flushScratch) < int(totalUsed) {
		ing.flushScratch = make([]byte, 0, totalUsed)
	}

	// Copy each sub-region's written slice in order.
	for ix := range ing.subRegions {
		lower := ing.subRegions[ix].Lower
		upper := ing.subRegions[ix].Upper

		// Clamp and compute written range
		start := lower

		end := cursors[ix] // ← Use cached value (no atomic load)
		if end < start {
			end = start
		}

		if end > upper {
			end = upper
		}

		if start < end {
			ing.flushScratch = append(ing.flushScratch, a.buf[start:end]...)
		}
	}

	for len(ing.flushScratch) > 0 {
		bytesWritten, errWrite := ing.writer.Write(ing.flushScratch)
		if errWrite != nil {
			ing.Registry.loadError(errWrite)

			return
		}

		if bytesWritten == 0 {
			ing.Registry.loadError(errWriterNoProgress)

			return
		}

		ing.flushScratch = ing.flushScratch[bytesWritten:]
	}
}

// Multi-write.
//
// This is default flusher.
func (ing *Ingestor) flushArenaPerRegion(a *arena) {
	if a == nil {
		return
	}

	for ix := range ing.subRegions {
		lower := ing.subRegions[ix].Lower
		upper := ing.subRegions[ix].Upper

		end := a.subRegionCursors[ix].value.Load()

		// Skip empty sub-regions early (sparse-write optimization)
		if end <= lower {
			continue
		}

		if end > upper {
			end = upper
		}

		data := a.buf[lower:end]

		for len(data) > 0 {
			bytesWritten, errWrite := ing.writer.Write(data)
			if errWrite != nil {
				ing.Registry.loadError(errWrite)

				return
			}

			if bytesWritten == 0 {
				ing.Registry.loadError(errWriterNoProgress)

				return
			}

			data = data[bytesWritten:]
		}
	}
}

// flushOnShutdown flushes both arenas best-effort.
func (ing *Ingestor) flushOnShutdown() {
	firstSealed := ing.rotate() // First rotation: seal whatever is currently active (call it A).

	// Second rotation: seal the other arena (B) which just became active.
	// Any producer that got bumped from A by the first rotate and retried
	// into B will be captured here.
	secondSealed := ing.rotate()

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
	ing.active.Store(nil)

	// Flush second-sealed first (it became active most recently,
	// producers who retried land here — wait for them first).
	if secondSealed != nil {
		if errWriteSecond := ing.waitForWriters(secondSealed); errWriteSecond == nil {
			ing.flusher(secondSealed)
		} else {
			ing.Registry.Inc(TErrDroppedSealedData)
		}
	}

	// Flush first-sealed.
	if firstSealed != nil && firstSealed != secondSealed {
		if errWriteFirst := ing.waitForWriters(firstSealed); errWriteFirst == nil {
			ing.flusher(firstSealed)
		} else {
			ing.Registry.Inc(TErrDroppedSealedData)
		}
	}
}

func (ing *Ingestor) signalFlush() {
	select {
	case ing.chFlush <- struct{}{}:

	default: // signal already pending, consumer will handle it
	}
}
