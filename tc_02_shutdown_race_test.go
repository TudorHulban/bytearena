package bytearena

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_FlushOnShutdown_DoubleRotate_Race(t *testing.T) {
	var buf bytes.Buffer

	// 1. Initialize an ingestor with the default flusher (flushArenaPerRegion)
	ingestor, err := NewIngestor(_Size1K, &buf)
	require.NoError(t, err)
	require.NotNil(t, ingestor)

	// Keep a reference to the data payload we expect to survive
	payload := []byte("critical_shutdown_log_data")

	var wgProducer sync.WaitGroup
	wgProducer.Add(1)

	// Step 2: Simulate a concurrent producer hitting the hot path
	// at the exact millisecond shutdown is called.
	go func() {
		defer wgProducer.Done()

		// Allocate space on the currently active arena (Arena A)
		region, errWrite := ingestor.beginWrite(uint32(len(payload)))
		if errWrite != nil {
			return
		}

		// INTENTIONAL DELAY: Simulate a context switch or slight hardware scheduling jitter
		// right after the producer successfully checked out its region but before completing the write.
		time.Sleep(5 * time.Millisecond)

		copy(region.Buf(), payload)
		ingestor.EndWrite(region)
	}()

	// Give the producer thread just enough time to clear beginWrite()
	// and enter its internal sleep window.
	time.Sleep(2 * time.Millisecond)

	// Step 3: Trigger your current flushOnShutdown() implementation concurrently.
	// This will execute:
	//   1. firstSealed := ing.rotate()  -> Swaps active from A to B
	//   2. secondSealed := ing.rotate() -> Swaps active from B back to A!
	//   3. ing.active.Store(nil)
	ingestor.flushOnShutdown()

	// Wait for the producer to completely finish its trailing execution path
	wgProducer.Wait()

	// Step 4: Core Assertion
	// If the double-rotate wrapped the pointer back and dropped Arena A's payload,
	// or if the flusher read the buffer while the delayed producer was still copying,
	// this will either panic, fail the race detector, or fail this assertion.
	require.Contains(t, buf.String(), string(payload),
		"CRITICAL FAILURE: Inflight data was dropped or corrupted during shutdown due to double pointer wrapping!")
}
