package bytearena

import (
	"context"
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/tudorhulban/bytearena/helpers"
)

// memory fieldalignment is entirely ignorant of cache lines.
// It has one job: minimize the GC pointer scan range ("pointer bytes"),
// which is the span from offset 0 to the last pointer-containing field.
// It achieves this by packing all pointer fields contiguously with zero gaps.
// The [56]byte pad between active and the cold pointer block is what it is complaining about.
// The gap inflates the GC bitmap by 56 bytes even though it contains no pointers.

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
type Ingestor struct { //nolint:govet
	// ── Cache line 0 ─────────────────────────────── Hot ──
	// Producers read this atomically on every write.
	active atomic.Pointer[arena]
	_      [55]byte

	// ── Cache line 1 ─────────────────────────────── Hot ──
	counterRequests atomic.Uint64
	_               [56]byte

	// ── Cache line 2 ─────────────────────────── Cold / IO ──
	// 3×io.Writer(16) + func(8) + chan(8) = 64 B exact.
	writer          io.Writer
	writerTelemetry io.Writer
	writerErrors    io.Writer
	flusher         func(a *arena)
	chFlush         chan struct{}

	// ── Cache line 3 ──────────────────────── Cold / Arena ──
	// 8+8+32+4+4+4+2+2 = 64 B exact, zero waste.
	arenaFirst             *arena
	arenaSecond            *arena
	arenaSealThresholds    [8]uint32
	arenaSize              uint32
	maxMessageSize         uint32
	arenaSealPercentage    uint32
	millisecondsTickIfData uint16
	millisecondsUnblock    uint16

	// ── Cache line 4 ─────────────────────── Cold / Consumer ──
	// flushScratch written every flush cycle (consumer-only).
	// Isolated here so flush writes never dirty the hot producer lines (0–2).
	// millisecondsTickThreshold co-located: same cold init-time access pattern.
	// 24 + 1 + 39 = 64 B exact.
	flushScratch              []byte // 24 bytes
	millisecondsTickThreshold uint8  //  1 byte
	withTelemetry             bool   // 1 byte
	_                         [38]byte

	// ── Cache line 5 ─────────────────────── Cold / SubRegions ──
	// Read-only after init. 8×{Lower,Upper uint32} = 64 B exact.
	subRegions [8]subRegion

	// ── Cache line 6+ ──────────────────────────────── Cold ──
	Registry ErrorsRegistry // 10×64 = 640 B, internally cache-line padded
	Metrics  Metrics        //         16 B

	counter *helpers.CPUCounter
	noCores uint8
}

// TODO:
// NewIngestor1Core
// NewIngestorMultipleCores
// NewIngestorAutoadjust

// NewIngestor allocates two arenas of the given size and initializes
// the Manager with a0 as the active arena and a1 as the standby arena.
func NewIngestor(arenaSize uint32, w io.Writer, options ...Options) (*Ingestor, error) {
	subRegions, regionSize := newSubRegions(arenaSize)

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

		arenaSealPercentage:       90,
		millisecondsTickIfData:    1000,
		millisecondsTickThreshold: 50,
		millisecondsUnblock:       50,

		counter: helpers.NewCPUCounter(),
		noCores: uint8(runtime.NumCPU()),
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
func (ing *Ingestor) StartIngestion(ctx context.Context) <-chan struct{} {
	chIngestionEnd := make(chan struct{})

	go func() {
		defer close(chIngestionEnd)

		ing.consumerLoop(ctx)
	}()

	return chIngestionEnd
}

// consumerLoop is the main consumer goroutine.
// It monitors the active arena, seals it when needed, waits for writers,
// flushes it, and resets it.
//
// This is only the skeleton — flushing and thresholds are implemented elsewhere.
func (ing *Ingestor) consumerLoop(ctx context.Context) {
	tickerThreshold := time.NewTicker(
		time.Duration(ing.millisecondsTickThreshold) * time.Millisecond,
	)
	defer tickerThreshold.Stop()

	tickerIfData := time.NewTicker(
		time.Duration(ing.millisecondsTickIfData) * time.Millisecond,
	)
	defer tickerIfData.Stop()

	chDone := ctx.Done() // Hoist the channel helps the compiler optimize the select case.

	for {
		select {
		case <-chDone:
			// Shutdown: flush both arenas best-effort.
			ing.flushOnShutdown()

			return

		case <-tickerThreshold.C:
			ing.tickThreshold()

			// consumerLoop gets a third case:
		case <-ing.chFlush:
			ing.tickThreshold() // same seal/wait/flush/reset as ticker path

		case <-tickerIfData.C:
			ing.tickIfData()
		}
	}
}
