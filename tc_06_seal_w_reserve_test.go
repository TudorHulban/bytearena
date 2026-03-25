package bytearena

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test Case 06: Race Between Reserve and Seal

// Test: Producer reserves space exactly as consumer seals
// Verifies: No writes to arena after it is sealed
func TestReserveVsSealRace(t *testing.T) {
	ingestor, errCrIngestor := NewIngestor(_Size1K, &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Channel to coordinate race
	chReady := make(chan struct{})
	chDone := make(chan bool)

	// Producer goroutine
	go func() {
		<-chReady // Wait for signal

		// Attempt to reserve
		region, errWrite := ingestor.beginWrite(100)
		if errWrite == nil {
			// If we got a region, it must be in active arena
			if region.arena != ingestor.active.Load() {
				chDone <- false

				return
			}

			ingestor.EndWrite(region)
		}

		chDone <- true
	}()

	// Consumer goroutine
	go func() {
		<-chReady // Wait for same signal

		// Rotate arenas
		sealed := ingestor.rotate()
		_ = sealed
	}()

	// Start both simultaneously
	close(chReady)

	// Wait for result
	require.True(t, <-chDone)

	// Verify invariant: No writes to sealed arena
	sealed := ingestor.sealed.Load()

	if sealed != nil {
		require.True(t,
			sealed.numberWriters.Load() == 0 || ingestor.active.Load() == sealed,
		)
	}
}
