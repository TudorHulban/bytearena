package bytearena

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test Case: Context Cancellation During Wait

// Test: Consumer context cancelled while waiting for writers.
// Verifies: Shutdown happens promptly, no hangs.
func TestContextCancelDuringWait(t *testing.T) {
	ingestor, errCrIngestor := NewIngestor(_Size1K, &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Start a write that never completes
	region, errWrite := ingestor.beginWrite(100)
	require.NoError(t, errWrite)

	// Don't call EndWrite() - simulate stuck producer

	// Rotate arena
	sealed := ingestor.rotate()
	require.Equal(t, ingestor.arenaFirst, sealed)

	// Start consumer with short-lived context
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	chConsumerExit := make(chan struct{})

	go func() {
		ingestor.consumerLoop(ctx)

		close(chConsumerExit)
	}()

	// Should exit within timeout, not hang forever
	select {
	case <-chConsumerExit:
		// Success

	case <-time.After(150 * time.Millisecond):
		t.Fatal("Consumer did not exit after context cancel")
	}

	// Clean up stuck producer
	ingestor.EndWrite(region)
}
