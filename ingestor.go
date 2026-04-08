package bytearena

import (
	"context"
	"io"
	"os"
	"sync/atomic"
	"time"
)

// 1. The Double-Buffer "Handshake"
// By having arenaFirst and arenaSecond, the system can allow a Producer to keep writing at full speed to one buffer
// while the Consumer (the flusher) writes the other buffer to the io.Writer.

// The Benefit: No "Stop-the-world" moments.
// The system never has to pause ingestion while waiting for a slow disk or network write,
// provided the flush finishes before the second arena fills up.

// 2. Atomic Pointer Management
// The use of atomic.Pointer[arena] is the modern, idiomatic Go way to handle high-concurrency state.
// In traditional code, we could use a sync.Mutex to swap buffers.
// In high-concurrency, that mutex becomes a bottleneck (contention).
// Using atomic.Pointer allows the "Swap" to happen in a single CPU cycle without blocking any writers.

// Ingestor owns the two arenas and coordinates which one is active.
// It also handles the rotation and flush on context cancellation.
type Ingestor struct {
	writer          io.Writer
	writerTelemetry io.Writer
	writerErrors    io.Writer

	flusher func(a *arena)

	chFlush chan struct{}

	// Pointer to the currently active arena.
	// Producers read this atomically to know where to write.
	active atomic.Pointer[arena]

	// The two arenas used in double-buffer rotation.
	arenaFirst  *arena
	arenaSecond *arena

	arenaSealThresholds []uint32 // precomputed

	Registry ErrorsRegistry
	Metrics  Metrics

	counterRequests atomic.Uint64
	subRegions      [8]SubRegion

	// Size of each arena (capacity of Arena.Buf).
	arenaSize      uint32
	maxMessageSize uint32

	arenaSealPercentage uint32

	milisecondsTickInterval uint16
	milisecondsUnblock      uint16

	isStopped     atomic.Bool
	withTelemetry bool
}

// NewIngestor allocates two arenas of the given size and initializes
// the Manager with a0 as the active arena and a1 as the standby arena.
func NewIngestor(arenaSize uint32, w io.Writer, options ...Options) (*Ingestor, error) {
	subRegions, regionSize := NewSubRegions(arenaSize)

	result := Ingestor{
		writer:          w,
		writerTelemetry: w,
		writerErrors:    os.Stdout,

		chFlush: make(chan struct{}, 1),

		arenaFirst:  newArena(arenaSize, subRegions),
		arenaSecond: newArena(arenaSize, subRegions),

		subRegions: subRegions,

		arenaSize:      arenaSize,
		maxMessageSize: regionSize,

		arenaSealPercentage:     90,
		milisecondsTickInterval: 50,
		milisecondsUnblock:      50,
	}

	for ix := range result.subRegions {
		result.
			arenaFirst.
			subRegionCursors[ix].value.Store(result.subRegions[ix].Lower)

		result.
			arenaSecond.
			subRegionCursors[ix].value.Store(result.subRegions[ix].Lower)
	}

	result.flusher = result.flushArenaPerRegion

	for _, option := range options {
		if errOption := option(&result); errOption != nil {
			return nil,
				errOption
		}
	}

	if result.withTelemetry {
		result.
			arenaFirst.
			telemetryObservableRollback = result.Metrics.IncrementRollback

		result.
			arenaSecond.
			telemetryObservableRollback = result.Metrics.IncrementRollback
	}

	// optimization - Precompute the subregions seal thresholds
	result.arenaSealThresholds = precomputeThresholds(
		result.subRegions,
		result.arenaSealPercentage,
	)

	// Set active arena to a0.
	result.active.Store(result.arenaFirst)

	return &result,
		nil
}

// StartIngestion launches the consumer loop in a goroutine.
// The caller provides the flush function, which receives the
// raw bytes of each sealed arena.
func (m *Ingestor) StartIngestion(ctx context.Context) <-chan struct{} {
	chIngestionEnd := make(chan struct{})

	go func() {
		defer close(chIngestionEnd)

		m.consumerLoop(ctx)
	}()

	return chIngestionEnd
}

// consumerLoop is the main consumer goroutine.
// It monitors the active arena, seals it when needed, waits for writers,
// flushes it, and resets it.
//
// This is only the skeleton — flushing and thresholds are implemented elsewhere.
func (m *Ingestor) consumerLoop(ctx context.Context) {
	ticker := time.NewTicker(
		time.Duration(m.milisecondsTickInterval) * time.Millisecond,
	)
	defer ticker.Stop()

	chDone := ctx.Done() // Hoist the channel helps the compiler optimize the select case.

	for {
		select {
		case <-chDone:
			// Shutdown: flush both arenas best-effort.
			m.isStopped.Store(true)
			m.flushOnShutdown()

			return

		case <-ticker.C:
			m.tick()

			// consumerLoop gets a third case:
		case <-m.chFlush:
			m.tick() // same seal/wait/flush/reset as ticker path
		}
	}
}
