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

// Test Case: Memory Corruption Check

// Test: Concurrent writes do not corrupt each other's data
// Verifies: Each log entry remains intact and contiguous.
// Enhanced version with write validation.
func TestNoMemoryCorruption_Enhanced(t *testing.T) {
	const arenaSize = 64 * _Size1K

	ingestor, errCrIngestor := NewIngestor(arenaSize, &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	chValidation := make(chan string, 10000)

	ingestor.flusher = func(a *arena) {
		// Use the new helper to extract all data from sharded subregions
		data := ingestor.getArenaData(a)

		// Validate all collected data line-by-line
		scanner := bufio.NewScanner(bytes.NewReader(data.Bytes()))
		for scanner.Scan() {
			line := string(scanner.Bytes())
			if line != "" {
				chValidation <- line
			}
		}

		ingestor.flushArena(a)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Consumer with validation
	go func() {
		for line := range chValidation {
			if !strings.HasPrefix(line, "P") {
				t.Errorf("Invalid line format: %q", line)
			}
		}
	}()

	var wgConsumer sync.WaitGroup
	wgConsumer.Go(
		func() {
			ingestor.consumerLoop(ctx)
		},
	)

	noProducers := 20

	var wgProducers sync.WaitGroup
	wgProducers.Add(noProducers)

	// Each producer writes a unique pattern
	for ix := range noProducers {
		go func(producerID int) {
			defer wgProducers.Done()

			for j := range 1000 {
				payload := fmt.Sprintf(
					"P%d-%d-%s",
					producerID,
					j,
					strings.Repeat("x", 50),
				)

				ingestor.write(
					uint32(len(payload)),
					func(dst []byte) {
						if len(dst) != len(payload) {
							t.Errorf(
								"Buffer size mismatch: got %d, want %d",
								len(dst),
								len(payload),
							)
						}

						copy(dst, payload)
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
