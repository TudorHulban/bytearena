package bytearena

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_02_Integration_ArenaFull_Size1K_Resilience(t *testing.T) {
	writer := bytes.Buffer{}

	// 1. Initialize the Ingestor with exact _Size1K configuration.
	// With 8 subregions, each subregion has exactly 128 bytes of space.
	ingestor, errCrIngestor := NewIngestor(
		_Size1K,
		&writer,
	)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	// Track the initial active arena pointer to verify automatic rotation later
	initialActiveArena := ingestor.active.Load()

	// 2. Prepare a 100-byte payload.
	// Since subregions are 128 bytes, a single 100-byte write fills 78% of a subregion,
	// guaranteed to blow past your lower/upper thresholds and trigger 'tickThreshold()'.
	payloadSize := 100
	payload := bytes.Repeat([]byte("x"), payloadSize)

	// We fire 12 concurrent producers.
	// Because of the CPU-sharded counter, they will spread
	// across shards, safely clustering and filling multiple 128-byte slots simultaneously.
	var wgProducers sync.WaitGroup

	noProducers := 12

	wgProducers.Add(noProducers)

	for range noProducers {
		go func() {
			defer wgProducers.Done()

			// TryWrite simulates ultra-low latency hot path allocation
			region, errWrite := ingestor.TryWrite(uint32(len(payload)))
			if errWrite != nil {
				// If a specific subregion fills completely before the ticker rotates,
				// TryWrite safely returns an error. This is expected behavior and prevents corruption.
				return
			}

			copy(region.Buf(), payload)
			ingestor.EndWrite(region)
		}()
	}

	// Wait for the saturating wave of writes to finish executing
	wgProducers.Wait()

	// 3. Give the background Go goroutines running tickThreshold() or tickIfData()
	// a small window (10ms) to wake up, detect the massive 100-byte entries,
	// execute rotate(), run waitForWriters(), and flush the data out to the writer.
	time.Sleep(10 * time.Millisecond)

	// 4. VALIDATE AUTOMATIC ROTATION:
	// Prove that the background ticker successfully hijacked the saturated arena
	// and swapped 'ingestor.active' over to the secondary arena entirely on its own.
	currentActiveArena := ingestor.active.Load()

	// Compares raw pointer memory addresses. Safe from data races.
	require.True(t, initialActiveArena != currentActiveArena,
		"Expected active arena pointer to change from %p to %p",
		initialActiveArena,
		currentActiveArena,
	)

	// 5. PROVE WRITING NEVER STOPS:
	// Write an overflow payload. This MUST succeed immediately because 'ingestor.active'
	// is now pointing to an unallocated secondary arena.
	overflowPayload := []byte("overflow_success_on_second_arena")
	region, errOverflow := ingestor.TryWrite(uint32(len(overflowPayload)))

	require.NoError(t,
		errOverflow,
		"The ingestor locked up! Writing should be unblocked on the new arena.",
	)
	require.Equal(t,
		currentActiveArena,
		region.arena,
		"New hot-path data must land on the rotated arena",
	)

	copy(region.Buf(), overflowPayload)
	ingestor.EndWrite(region)

	// 6. Graceful Shutdown
	cancel()
	<-chIngestionEnd

	// 7. FINAL INTEGRITY CHECK:
	// Prove that the data written to the fresh arena after the overflow survived
	// the rotation state change and was cleanly flushed down into the final stream.
	require.Contains(t,
		writer.String(),
		"overflow_success_on_second_arena",
	)
}
