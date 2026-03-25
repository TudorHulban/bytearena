package bytearena

import (
	"context"
	"io"
	"sync/atomic"
	"time"
)

// Ingestor owns the two arenas and coordinates which one is active.
// It also handles the rotation and flush on context cancellation.
type Ingestor struct {
	writer io.Writer

	chFlush chan struct{}

	// Pointer to the currently active arena.
	// Producers read this atomically to know where to write.
	active atomic.Pointer[arena]

	// The two arenas used in double-buffer rotation.
	arenaFirst  *arena
	arenaSecond *arena

	// The arena currently sealed and waiting to be flushed.
	// This is informational; consumer logic will use it.
	sealed atomic.Pointer[arena]

	// Size of each arena (capacity of Arena.Buf).
	arenaSize           uint32
	arenaSealPercentage uint32
}

// NewIngestor allocates two arenas of the given size and initializes
// the Manager with a0 as the active arena and a1 as the standby arena.
func NewIngestor(arenaSize uint32, w io.Writer, options ...Options) (*Ingestor, error) {
	result := Ingestor{
		arenaFirst: &arena{
			buf: make([]byte, arenaSize),
		},
		arenaSecond: &arena{
			buf: make([]byte, arenaSize),
		},

		arenaSize:           arenaSize,
		arenaSealPercentage: 90,
		writer:              w,

		chFlush: make(chan struct{}, 1),
	}

	for _, option := range options {
		if errOption := option(&result); errOption != nil {
			return nil,
				errOption
		}
	}

	// Set active arena to a0.
	result.active.Store(result.arenaFirst)

	// No sealed arena yet.
	result.sealed.Store(nil)

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

		m.consumerLoop(
			ctx,

			func(a *arena) {
				m.flushArena(a)
			},
		)
	}()

	return chIngestionEnd
}

// consumerLoop is the main consumer goroutine.
// It monitors the active arena, seals it when needed, waits for writers,
// flushes it, and resets it.
//
// This is only the skeleton — flushing and thresholds are implemented elsewhere.
func (m *Ingestor) consumerLoop(ctx context.Context, flusher flusher) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Shutdown: flush both arenas best-effort.
			m.flushOnShutdown(ctx, flusher)

			return

		case <-ticker.C:
			m.tick(flusher)

			// consumerLoop gets a third case:
		case <-m.chFlush:
			m.tick(flusher) // same seal/wait/flush/reset as ticker path
		}
	}
}
