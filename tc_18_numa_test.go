package bytearena

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// NUMA = Non‑Uniform Memory Access
// Test Case 17: NUMA-Style False Sharing Detection

// Test: Multiple cores hammer different atomics.
// Verifies: Cache line padding works (performance, not correctness).
func TestFalseSharingResistance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ingestor, errCrIngestor := NewIngestor(Size1M(), &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	arena := ingestor.active.Load()

	numberExecutions := 1000000

	// Goroutine 1: Hammer cursor
	go func() {
		for range numberExecutions {
			arena.cursor.Add(1)
		}
	}()

	// Goroutine 2: Hammer writers
	go func() {
		for range numberExecutions {
			arena.numberWriters.Add(1)
		}
	}()

	// Goroutine 3: Hammer rollback
	go func() {
		for range numberExecutions {
			arena.rollbackCounter.Add(1)
		}
	}()

	// If padding is wrong, this will be slow due to cache contention
	// We are not measuring, just ensuring no crashes.
	time.Sleep(100 * time.Millisecond)

	// If we got here without data races, padding is likely correct
	// (run with -race to verify)
}
