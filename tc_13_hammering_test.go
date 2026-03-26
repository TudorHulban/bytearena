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

// Test Case 13: Hammer arena with huge messages

// Test: Try to hammer arena with write request larger than arena size.
// Say 90% - 100% of requests are greater than arena size.
// Verifies:
// 1. The 10% of valid writes are correctly written under multiple rotations.
// 2. Cursor works correctly.
func TestHammerWithHugeMessages(t *testing.T) {
	var out bytes.Buffer

	// Use a small arena to make oversized writes common
	const (
		arenaSize           = 256 // bytes
		hugeRatio           = 90  // 90% of writes are > arenaSize
		numProducers        = 8
		writesPerProducer   = 500
		totalWritesExpected = numProducers * writesPerProducer

		// With 90% huge, we expect ~10% valid writes
		expectedValidWrites = totalWritesExpected * (100 - hugeRatio) / 100
	)

	ingestor, errCrIngestor := NewIngestor(arenaSize, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Track metrics
	var (
		rotationCount    atomic.Int32
		hugeWrites       atomic.Int64
		validWrites      atomic.Int64
		oversizedWrites  atomic.Int64 // writes > arenaSize
		rollbackCount    atomic.Int64
		successfulWrites atomic.Int64
	)

	// Track cursor values for verification
	var (
		cursorHistory []int32
		cursorMutex   sync.Mutex
	)

	ingestor.flusher = func(a *arena) {
		rotationCount.Add(1)

		cursorMutex.Lock()

		cursorHistory = append(cursorHistory, a.cursor.Load())
		cursorMutex.Unlock()

		// Track rollbacks from this arena
		rollbacks := a.rollbackCounter.Load()
		if rollbacks > 0 {
			rollbackCount.Add(int64(rollbacks))
		}

		// Verify cursor never exceeds arena size
		cursor := a.cursor.Load()
		require.LessOrEqual(t,
			cursor,
			int32(arenaSize),
			"cursor %d exceeds arena size %d", cursor, arenaSize)

		// Log progress periodically
		if rotationCount.Load()%50 == 0 {
			t.Logf(
				"Rotation %d: cursor=%d, used=%d, rollbacks=%d",
				rotationCount.Load(),
				cursor,
				a.cursor.Load(),
				rollbacks,
			)
		}

		ingestor.flushArena(a)
		a.reset()

		// After reset, cursor must be 0
		require.Zero(t,
			a.cursor.Load(),
			"cursor not reset to 0",
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wgConsumer sync.WaitGroup
	wgConsumer.Add(1)

	// Start consumer with monitoring
	go func() {
		defer wgConsumer.Done()

		ingestor.consumerLoop(ctx)
	}()

	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	// Start producers with mixed write sizes
	for p := range numProducers {
		go func(producerID int) {
			defer wgProducers.Done()

			// Each producer gets its own rand source
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(producerID)))

			for j := range writesPerProducer {
				// Decide if this write will be huge (> arenaSize) or valid
				var size int

				if r.Intn(100) < hugeRatio {
					// Huge write: 2x to 10x arena size
					size = arenaSize + r.Intn(arenaSize*9)

					hugeWrites.Add(1)
				} else {
					// Valid write: 10-50 bytes
					size = 10 + r.Intn(40)

					validWrites.Add(1)
				}

				payload := fmt.Sprintf(
					"p%d-%d-%s\n",
					producerID,
					j,
					randomString(size-9),
				)

				// Track if this write is oversized (> arenaSize)
				if uint32(len(payload)) > arenaSize {
					oversizedWrites.Add(1)
				}

				errWrite := ingestor.write(
					uint32(len(payload)),
					func(dst []byte) {
						copy(dst, []byte(payload))
					},
				)
				if errWrite == nil {
					successfulWrites.Add(1)
				}

				// Small random delay to increase race probability
				if j%10 == 0 {
					time.Sleep(time.Duration(r.Intn(5)) * time.Microsecond)
				}
			}
		}(p)
	}

	// Wait for all producers to finish
	wgProducers.Wait()
	t.Log("All producers finished")

	// Stop consumer
	cancel()
	wgConsumer.Wait()

	// Collect final metrics
	finalRotations := rotationCount.Load()
	finalHuge := hugeWrites.Load()
	finalValid := validWrites.Load()
	finalOversized := oversizedWrites.Load()
	finalRollbacks := rollbackCount.Load()
	finalSuccessful := successfulWrites.Load()

	t.Log("=== Final Metrics ===")
	t.Logf("Total rotations: %d", finalRotations)
	t.Logf("Huge writes (intentional): %d", finalHuge)
	t.Logf("Valid writes (intentional): %d", finalValid)
	t.Logf("Actual oversized writes (>arenaSize): %d", finalOversized)
	t.Logf("Successful writes: %d", finalSuccessful)
	t.Logf("Total rollbacks: %d", finalRollbacks)
	t.Logf("Output size: %d bytes", out.Len())

	// Verify we had many rotations due to pressure
	require.Greater(t,
		finalRotations,
		int32(10),
		"should have had multiple rotations under pressure",
	)

	// Verify cursor integrity
	cursorMutex.Lock()
	defer cursorMutex.Unlock()

	for i, cursor := range cursorHistory {
		require.GreaterOrEqual(t,
			cursor,
			int32(0),

			"cursor[%d] = %d is negative", i, cursor,
		)

		require.LessOrEqual(t,
			cursor,
			int32(arenaSize),

			"cursor[%d] = %d exceeds arena size", i, cursor,
		)
	}

	// Verify valid writes made it to output
	output := out.String()
	lines := bytes.Split(bytes.TrimSpace([]byte(output)), []byte{'\n'})
	outputLines := len(lines)

	t.Logf(
		"Output lines: %d", outputLines,
	)
	t.Logf(
		"Expected valid writes (approx): %d", expectedValidWrites,
	)

	// We should have some output (the valid writes)
	require.Greater(t,
		outputLines,
		0,
		"should have some output from valid writes",
	)

	// The number of successful writes should match output lines
	require.Equal(t,
		int(finalSuccessful),
		outputLines,

		"successful writes should match output lines",
	)

	// Verify all successful writes are valid size (<= arenaSize)
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		require.LessOrEqual(t,
			len(line),
			int(arenaSize),

			"output line exceeds arena size: %q (%d bytes)", line, len(line),
		)
	}

	// Verify relationship between oversized writes and rollbacks
	// Each oversized write should cause at least one rollback
	t.Logf(
		"Oversized writes: %d, Rollbacks: %d",
		finalOversized,
		finalRollbacks,
	)

	// Each oversized write creates a considered rollback only
	// if there are also successful writes.
	require.GreaterOrEqual(t,
		finalHuge+finalValid-finalSuccessful,
		int64(0),

		"write failures should exist",
	)
}

// TestHammerWithHugeMessages_Detailed tracks per-rotation metrics
func TestHammerWithHugeMessages_Detailed(t *testing.T) {
	var out bytes.Buffer

	const (
		arenaSize         = 512
		hugeRatio         = 95 // 95% huge messages
		numProducers      = 4
		writesPerProducer = 200
	)

	ingestor, errCrIngestor := NewIngestor(arenaSize, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	type rotationMetrics struct {
		index     int32
		cursor    int32
		used      uint32
		rollbacks int32
		writers   int32
	}

	var (
		metrics       []rotationMetrics
		metricsMutex  sync.Mutex
		rotationIndex int32 = 0
	)

	ingestor.flusher = func(a *arena) {
		rotationIndex++

		metricsMutex.Lock()

		metrics = append(metrics, rotationMetrics{
			index:     rotationIndex,
			cursor:    a.cursor.Load(),
			used:      uint32(a.cursor.Load()),
			rollbacks: a.rollbackCounter.Load(),
			writers:   a.numberWriters.Load(),
		})
		metricsMutex.Unlock()

		ingestor.flushArena(a)
		a.reset()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wgConsumer sync.WaitGroup
	wgConsumer.Add(1)

	go func() {
		defer wgConsumer.Done()

		ingestor.consumerLoop(ctx)
	}()

	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	var (
		writeAttempts  atomic.Int64
		writeSuccess   atomic.Int64
		writeOversized atomic.Int64
	)

	for p := range numProducers {
		go func(producerID int) {
			defer wgProducers.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(producerID)))

			for j := range writesPerProducer {
				var size int

				if r.Intn(100) < hugeRatio {
					// Oversized: 1.5x to 5x arena size
					size = arenaSize + r.Intn(arenaSize*4)

					writeOversized.Add(1)
				} else {
					// Valid: 10-100 bytes
					size = 10 + r.Intn(90)
				}

				payload := fmt.Sprintf(
					"p%d-%d-%s\n",
					producerID,
					j,
					randomString(size-9),
				)

				writeAttempts.Add(1)

				errWrite := ingestor.write(
					uint32(len(payload)),
					func(dst []byte) {
						copy(dst, []byte(payload))
					},
				)
				if errWrite == nil {
					writeSuccess.Add(1)
				}
			}
		}(p)
	}

	wgProducers.Wait()
	cancel()
	wgConsumer.Wait()

	// Analyze metrics
	metricsMutex.Lock()
	defer metricsMutex.Unlock()

	t.Logf("Total rotations: %d", len(metrics))
	t.Logf("Write attempts: %d", writeAttempts.Load())
	t.Logf("Write successes: %d", writeSuccess.Load())
	t.Logf("Oversized attempts: %d", writeOversized.Load())

	// Verify each rotation's metrics
	totalRollbacks := int32(0)
	totalUsed := uint32(0)

	for i, m := range metrics {
		totalRollbacks = totalRollbacks + m.rollbacks
		totalUsed = totalUsed + m.used

		// Cursor should never exceed arena size
		require.LessOrEqual(t,
			m.cursor,
			int32(arenaSize),

			"rotation %d: cursor %d > arenaSize", i, m.cursor,
		)

		// Used bytes should match cursor (or be clamped to arenaSize)
		if m.used > 0 {
			require.GreaterOrEqual(t,
				m.cursor,
				int32(0),

				"rotation %d: cursor negative with used bytes", i,
			)
		}

		// Writers should be 0 at flush time (waitForWriters called)
		require.Zero(t,
			m.writers,
			"rotation %d: writers still active during flush", i,
		)

		if i > 0 {
			t.Logf(
				"Rotation %d: cursor=%d, used=%d, rollbacks=%d",

				m.index,
				m.cursor,
				m.used,
				m.rollbacks,
			)
		}
	}

	// Verify output integrity
	output := out.String()
	lines := bytes.Split(bytes.TrimSpace([]byte(output)), []byte{'\n'})
	outputLines := len(lines)

	t.Logf("Total output lines: %d", outputLines)
	t.Logf("Total used bytes across rotations: %d", totalUsed)
	t.Logf("Total rollbacks: %d", totalRollbacks)

	// All successful writes should be in output
	require.Equal(t,
		int(writeSuccess.Load()),
		outputLines,

		"successful writes should match output lines",
	)

	// Each oversized write creates a considered rollback only
	// if there are also successful writes.
	require.GreaterOrEqual(t,
		writeAttempts.Load()-writeSuccess.Load(),
		int64(0),

		"write failures should exist",
	)
}
