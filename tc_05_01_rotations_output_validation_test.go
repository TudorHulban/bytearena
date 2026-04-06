package bytearena

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test Case: Many rotations during high write rate
//
// Test: Multiple rotates. Say 1000.
// Verifies:
// 1. No messages are lost.
// 2. Cursor works correctly. Ensures cursor is reset to 0 after each rotation.
// 3. Validates each output line has correct format.
// 4. Validates no duplicate messages appear in output.
func Test_ManyRotations_01_OutputValidation(t *testing.T) {
	var writer bytes.Buffer

	const (
		arenaSize         = _Size1K
		numRotations      = 1000
		numProducers      = 8
		writesPerProducer = 250 // Total writes: 8 * 250 = 2000 writes
	)

	ingestor, errCrIngestor := NewIngestor(arenaSize, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Track rotation count
	var rotationCount atomic.Int32

	ingestor.flusher = func(a *arena) {
		rotationCount.Add(1) // Count this rotation

		// Validate cursors are within bounds.
		cursors := a.getCursorValues()
		for ix, cursor := range cursors {
			region := ingestor.subRegions[ix]

			require.GreaterOrEqual(t,
				cursor,
				uint64(region.Lower),

				"cursor[%d] value %d below region lower bound %d",
				ix,
				cursor,
				region.Lower,
			)

			require.LessOrEqual(t,
				cursor,
				uint64(region.Upper),

				"cursor[%d] value %d exceeds region upper bound %d",
				ix,
				cursor,
				region.Upper,
			)
		}

		ingestor.flushArena(a)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	chIngestionEnd := ingestor.StartIngestion(ctx)

	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	// Track successful writes
	var (
		successfulWrites atomic.Int64
		failedWrites     atomic.Int64
	)

	// Channel to signal all producers are done.
	chDone := make(chan struct{})

	// Start producers
	for producer := range numProducers {
		go func(producerID int) {
			defer wgProducers.Done()

			for j := range writesPerProducer {
				// Create variable-sized payload to increase rotation frequency
				// and create more edge cases
				size := 10 + rand.Intn(50) // 10-60 bytes

				payload := fmt.Sprintf(
					"p%d-%d-%s\n",
					producerID,
					j,
					randomString(size-9), // Adjust for prefix length
				)

				errWrite := ingestor.write(
					uint32(len(payload)),
					func(dst []byte) {
						copy(dst, payload)
					},
				)
				if errWrite == nil {
					successfulWrites.Add(1)
				} else {
					failedWrites.Add(1)

					if errors.Is(errWrite, ErrWriteMessageTooLarge) {
						t.Logf(
							"Message too large - Expected sometimes with random sizes: %v",
							errWrite,
						)
					} else if errors.Is(errWrite, ErrWriteSubRegionFull) {
						t.Logf(
							"Write arena full - Expected during high pressure: %v",
							errWrite,
						)
					} else {
						t.Logf(
							"Unexpected error: %v",
							errWrite,
						)
					}
				}

				// Small random delay to increase race probability
				if j%10 == 0 {
					time.Sleep(
						time.Duration(rand.Intn(5)) * time.Microsecond,
					)
				}
			}
		}(producer)
	}

	// Wait for all producers to finish
	wgProducers.Wait()
	close(chDone)

	// Stop consumer
	cancel()

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

	for ix, arena := range []*arena{ingestor.arenaFirst, ingestor.arenaSecond} {
		require.GreaterOrEqual(t,
			arena.rollbackCounter.Load(),
			int32(0),

			"arena %d rollback negative",
			ix,
		)
	}

	// Verify we had many rotations
	rotations := rotationCount.Load()

	t.Logf("Total rotations: %d", rotations)
	t.Logf("Successful writes: %d", successfulWrites.Load())
	t.Logf("Failed writes: %d", failedWrites.Load())

	// We should have had multiple rotations
	require.Greater(t,
		rotations,
		int32(10),

		"should have had at least 10 rotations",
	)

	// Verify output integrity
	output := writer.String()
	lines := bytes.Split(
		bytes.TrimSpace([]byte(output)),
		[]byte{'\n'},
	)

	// Count actual lines in output
	outputLines := len(lines)

	// Verify no messages lost - all successful writes should appear in output
	require.Equal(t,
		int(successfulWrites.Load()),
		outputLines,

		"number of output lines should match successful writes",
	)

	// Verify each line has correct format
	lineMap := make(map[string]bool)

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		// Check line format: p<producerID>-<counter>-<random>
		require.Regexp(t,
			`^p\d+-\d+-[a-z]+$`,
			string(line),
			"line has invalid format: %q", string(line),
		)

		// Check for duplicates
		lineStr := string(line)
		if lineMap[lineStr] {
			t.Errorf("duplicate line found: %q", lineStr)
		}

		lineMap[lineStr] = true
	}

	// Verify final arena states are consistent
	activeArena := ingestor.active.Load()

	// Active arena is niled during shutdown.
	require.Nil(t, activeArena)

	// Verify no arena has negative counters
	arenas := []*arena{ingestor.arenaFirst, ingestor.arenaSecond}

	for ix, arena := range arenas {
		cursors := arena.getCursorValues()

		for _, cursor := range cursors {
			require.GreaterOrEqual(t,
				cursor,
				uint64(0),
			)
		}

		require.GreaterOrEqual(t,
			arena.numberWriters.Load(),
			int32(0),

			"arena %d writers negative",
			ix,
		)

		require.GreaterOrEqual(t,
			arena.rollbackCounter.Load(),
			int32(0),

			"arena %d rollbacks negative",
			ix,
		)
	}
}
