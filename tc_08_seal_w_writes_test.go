package bytearena

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test Case: Seal During Active Writes

// Test: Consumer seals arena while producers are in middle of writing
// Verifies: In-flight writes complete successfully, no writes to sealed arena
func Test_08_SealDuringActiveWrites(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(Size100K(), &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	var wgProducers sync.WaitGroup

	noProducers := 12 // Start producers that write slowly

	chWritesStarted := make(chan struct{}, noProducers)

	wgProducers.Add(noProducers)

	for range noProducers {
		go func() {
			defer wgProducers.Done()

			// Slow write that takes time
			region, errWrite := ingestor.fnBeginWrite(ingestor, 100)
			if errWrite != nil {
				t.Errorf(
					"beginWrite failed unexpectedly: %v",
					errWrite,
				)

				return
			}

			chWritesStarted <- struct{}{}

			// Simulate slow write (50ms)
			time.Sleep(50 * time.Millisecond)

			// Write data
			copy(region.Buf(), bytes.Repeat([]byte("x"), 100))
			ingestor.EndWrite(region)
		}()
	}

	// Wait for all producers to start writing
	for range noProducers {
		<-chWritesStarted
	}

	// Seal arena while writes are in progress
	sealedArena := ingestor.rotate()
	require.NotNil(t, sealedArena)

	// no rollbacks should have occurred
	require.Zero(t, sealedArena.rollbackCounter.Load())

	// Try to write to active arena (should be new one)
	region, errWrite := ingestor.fnBeginWrite(ingestor, 10)
	require.NoError(t, errWrite)

	// Should be other arena
	require.Equal(t,
		ingestor.arenaSecond,
		region.arena,
	)
	ingestor.EndWrite(region)

	// Wait for all slow writes to complete
	wgProducers.Wait()

	// Verify: Sealed arena has writers=0
	require.EqualValues(t,
		0,
		sealedArena.numberWriters.Load(),
	)

	// Now safe to flush
	_, used := sealedArena.getSubregionLoads()

	ingestor.flushArenaPerRegion(sealedArena)
	require.EqualValues(t,
		used,
		writer.Len(),

		"used in sealed arena: %d different than what was written to writer: %d",
		used,
		writer.Len(),
	)

	sealedArena.reset()

	// Verify: All 5 writes were flushed
	require.Equal(t,
		noProducers*100,
		len(writer.String()),
	)
}
