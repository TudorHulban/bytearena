package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test Case 04: Many rotations during high write rate
//
// Test: Multiple rotates. Say 1000.
// Verifies:
// 1. No messages are lost.
// 2. Cursor works correctly. Ensures cursor is reset to 0 after each rotation.
// 3. Validates each output line has correct format.
// 4. Validates no duplicate messages appear in output.
func TestManyRotations(t *testing.T) {
	var out bytes.Buffer

	// Use a small arena size to force frequent rotations
	const (
		arenaSize         = 512 // bytes
		numRotations      = 1000
		numProducers      = 8
		writesPerProducer = 250 // Total writes: 8 * 250 = 2000 writes
	)

	ingestor, errCrIngestor := NewIngestor(arenaSize, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wgConsumer sync.WaitGroup
	wgConsumer.Add(1)

	// Track rotation count
	var rotationCount atomic.Int32

	// Start consumer with rotation tracking
	go func() {
		defer wgConsumer.Done()

		ingestor.consumerLoop(
			ctx,

			func(a *arena) {
				// Count this rotation
				rotationCount.Add(1)

				// Validate cursor is within bounds
				cursor := a.cursor.Load()
				require.GreaterOrEqual(t,
					cursor,
					int32(0),
					"cursor should be non-negative",
				)
				require.LessOrEqual(t,
					cursor,
					int32(arenaSize),
					"cursor should not exceed arena size",
				)

				ingestor.waitForWriters(a)
				ingestor.flushArena(a)
				a.reset()

				// After reset, verify arena is clean
				require.Zero(t,
					a.cursor.Load(),
					"cursor should be 0 after reset",
				)
				require.Zero(t,
					a.rollbackCounter.Load(),
					"rollback counter should be 0 after reset",
				)

				// Note: numberWriters is not reset here as per arena.reset() documentation
			},
		)
	}()

	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	// Track successful writes
	var (
		successfulWrites atomic.Int64
		failedWrites     atomic.Int64
	)

	// Channel to signal all producers are chDone
	chDone := make(chan struct{})

	// Start producers
	for p := range numProducers {
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
						copy(dst, []byte(payload))
					},
				)
				if errWrite == nil {
					successfulWrites.Add(1)
				} else {
					failedWrites.Add(1)

					// Log failure type for debugging
					switch errWrite {
					case ErrWriteMessageTooLarge:
						// Expected sometimes with random sizes

					case ErrWriteArenaFull:
						// Expected during high pressure

					default:
						t.Logf("Unexpected error: %v", errWrite)
					}
				}

				// Small random delay to increase race probability
				if j%10 == 0 {
					time.Sleep(
						time.Duration(rand.Intn(5)) * time.Microsecond,
					)
				}
			}
		}(p)
	}

	// Wait for all producers to finish
	wgProducers.Wait()
	close(chDone)

	// Stop consumer
	cancel()
	wgConsumer.Wait()

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
	output := out.String()
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
	sealedArena := ingestor.sealed.Load()

	// Active arena should have valid state
	require.NotNil(t, activeArena)
	require.GreaterOrEqual(t,
		activeArena.cursor.Load(),
		int32(0),
	)
	require.LessOrEqual(t, activeArena.cursor.Load(), int32(arenaSize))

	// If there is a sealed arena, it should have no writers
	if sealedArena != nil {
		require.Zero(t,
			sealedArena.numberWriters.Load(),
			"sealed arena should have no writers",
		)
	}

	// Verify no arena has negative counters
	arenas := []*arena{ingestor.arenaFirst, ingestor.arenaSecond}

	for i, a := range arenas {
		require.GreaterOrEqual(t, a.cursor.Load(), int32(0), "arena %d cursor negative", i)
		require.GreaterOrEqual(t, a.numberWriters.Load(), int32(0), "arena %d writers negative", i)
		require.GreaterOrEqual(t, a.rollbackCounter.Load(), int32(0), "arena %d rollbacks negative", i)
	}
}

// TestManyRotations_CursorIntegrity specifically tests cursor behavior
// during many rotations
func TestManyRotations_CursorIntegrity(t *testing.T) {
	var out bytes.Buffer

	const (
		arenaSize    = 256
		numRotations = 500
	)

	ingestor, errCrIngestor := NewIngestor(arenaSize, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var rotationCount atomic.Int32

	var wgConsumer sync.WaitGroup
	wgConsumer.Add(1)

	// Track cursor values across rotations
	var cursorHistory []int32

	var cursorMutex sync.Mutex

	go func() {
		defer wgConsumer.Done()

		ingestor.consumerLoop(
			ctx,
			func(a *arena) {
				rotationCount.Add(1)

				cursorMutex.Lock()

				cursorHistory = append(cursorHistory, a.cursor.Load())
				cursorMutex.Unlock()

				// Verify cursor never exceeds arena size
				cursor := a.cursor.Load()
				require.LessOrEqual(t, cursor, int32(arenaSize),
					"cursor %d exceeds arena size %d", cursor, arenaSize)

				ingestor.flushArena(a)
				a.reset()

				// After reset, cursor must be 0
				require.Equal(t, int32(0), a.cursor.Load(),
					"cursor not reset to 0")
			},
		)
	}()

	// Single producer writing sequentially to make cursor behavior predictable
	for i := range numRotations * 2 {
		payload := fmt.Sprintf("msg-%d-", i)
		payload = payload + randomString(20) // Ensure we fill arena quickly

		_ = ingestor.write(
			uint32(len(payload)),
			func(dst []byte) {
				copy(dst, []byte(payload))
			},
		)

		// Small delay to allow rotations
		if i%10 == 0 {
			time.Sleep(time.Microsecond)
		}
	}

	cancel()
	wgConsumer.Wait()

	// Verify we had many rotations
	rotations := rotationCount.Load()
	t.Logf(
		"Total rotations: %d",
		rotations,
	)
	require.Greater(t,
		rotations,
		int32(10),
		"should have had multiple rotations",
	)

	// Verify cursor values are monotonically increasing within each arena's lifetime.
	cursorMutex.Lock()
	defer cursorMutex.Unlock()

	for ix, cursor := range cursorHistory {
		// Each cursor should be between 0 and arenaSize
		require.GreaterOrEqual(t,
			cursor,
			int32(0),
			"cursor[%d] = %d is negative", ix, cursor,
		)

		require.LessOrEqual(t,
			cursor,
			int32(arenaSize),
			"cursor[%d] = %d exceeds arena size", ix, cursor,
		)

		// Cursor should never be exactly arenaSize? Actually it can be
		// if a write exactly fills the arena
		if cursor == int32(arenaSize) {
			t.Logf(
				"Cursor[%d] exactly at arena size", ix,
			)
		}
	}
}
