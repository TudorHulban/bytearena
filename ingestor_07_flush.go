package bytearena

import (
	"context"
	"fmt"
	"time"
)

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

	isolatedBuffer := make([]byte, used)
	copy(isolatedBuffer, a.buf[:used])

	for len(isolatedBuffer) > 0 {
		bytesWritten, errWrite := m.writer.Write(isolatedBuffer)

		// Partial writes are allowed even when err != nil.
		// We stop because the caller cannot recover meaningfully.
		if errWrite != nil {
			m.Registry.loadError(errWrite)

			fmt.Fprintf(
				m.writerErrors,
				"flushArena: %s\n",
				errWrite.Error(),
			)

			return
		}

		if bytesWritten == 0 {
			// Zero progress → abort
			_, _ = m.writerErrors.Write(
				[]byte("writer made zero progress\n"),
			)

			return
		}

		isolatedBuffer = isolatedBuffer[bytesWritten:]
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
