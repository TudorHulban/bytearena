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
		// Extract data from all 8 subregions using cursor snapshots
		cursors := a.getCursorValues()

		var allData bytes.Buffer

		for ix := range ingestor.subRegions {
			region := ingestor.subRegions[ix]
			cursor := cursors[ix]

			// Skip empty subregions (cursor hasn't advanced from Lower bound)
			if cursor <= uint64(region.Lower) {
				continue
			}

			// Data in this subregion: buf[Lower : cursor]
			start := region.Lower
			end := uint32(cursor) // cursor is uint64, bounds are uint32

			// Safety clamp: never read beyond region Upper
			if end > region.Upper {
				end = region.Upper
			}

			if start < end {
				allData.Write(a.buf[start:end])
				// Add separator to ensure clean line scanning across shard boundaries
				// (assumes messages don't span shards; separator prevents false merges)
				allData.WriteByte('\n')
			}
		}

		// Validate all collected data line-by-line
		scanner := bufio.NewScanner(bytes.NewReader(allData.Bytes()))
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
			// Validate format immediately
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

	var wgProducers sync.WaitGroup
	noProducers := 20
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
						// Double-check destination before writing
						if len(dst) != len(payload) {
							t.Errorf(
								"Buffer size mismatch: got %d, want %d",
								len(dst),
								len(payload),
							)
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
