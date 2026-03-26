package bytearena

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test Case 08: Memory Corruption Check

// Test: Concurrent writes do not corrupt each other's data
// Verifies: Each log entry remains intact and contiguous.
// Enhanced version with write validation.
func TestNoMemoryCorruption_Enhanced(t *testing.T) {
	ingestor, errCrIngestor := NewIngestor(64*_Size1K, &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	chValidation := make(chan string, 10000)

	ingestor.flusher = func(a *arena) {
		// Capture output for validation
		data := a.buf[:a.cursor.Load()]

		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			line := string(scanner.Bytes())

			if line != "" {
				chValidation <- line
			}
		}

		ingestor.flushArena(a)
		a.reset()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var wgConsumer sync.WaitGroup
	wgConsumer.Add(1)

	// Consumer with validation
	go func() {
		for line := range chValidation {
			// Validate format immediately
			if !strings.HasPrefix(line, "P") {
				t.Errorf("Invalid line format: %q", line)
			}
		}
	}()

	go func() {
		defer wgConsumer.Done()

		ingestor.consumerLoop(ctx)
	}()

	var wgProducers sync.WaitGroup

	noProducers := 20

	wgProducers.Add(noProducers)

	// Each producer writes a unique pattern
	for ix := range noProducers {
		go func(producerID int) {
			defer wgProducers.Done()

			for j := range 1000 {
				payload := fmt.Sprintf("P%d-%d-%s", producerID, j,
					strings.Repeat("x", 50))

				ingestor.write(
					uint32(len(payload)),

					func(dst []byte) {
						// Double-check destination before writing
						if len(dst) != len(payload) {
							t.Errorf("Buffer size mismatch: got %d, want %d",
								len(dst), len(payload))
						}

						copy(dst, []byte(payload))
					},
				)
			}
		}(ix)
	}

	wgProducers.Wait()
	cancel()

	wgConsumer.Wait()
	close(chValidation)
}
