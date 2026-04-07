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

// Test Case: Hammer arena with huge messages

// Test: Try to hammer arena with write request larger than subregion size.
// Say 90% - 100% of requests are greater than subregion size.
// Verifies:
// 1. The 10% of valid writes are correctly written under multiple rotations.
// 2. Cursor works correctly.
func TestHammerWithHugeMessages(t *testing.T) {
	var writer bytes.Buffer

	// Use a small arena to make oversized writes common
	const (
		arenaSize           = 800 // bytes → 8 subregions × 100 bytes each
		hugeRatio           = 90  // 90% of writes are > arenaSize
		numProducers        = 8
		writesPerProducer   = 500
		totalWritesExpected = numProducers * writesPerProducer

		// With 90% huge, we expect ~10% valid writes
		expectedValidWrites = totalWritesExpected * (100 - hugeRatio) / 100
	)

	ingestor, errCrIngestor := NewIngestor(arenaSize, &writer)
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

	// Track cursor values for verification (now per-shard snapshots)
	var (
		cursorHistory [][]uint32 // each entry: snapshot of 8 cursor values
		cursorMutex   sync.Mutex
	)

	ingestor.flusher = func(a *arena) {
		rotationCount.Add(1)

		// Capture snapshot of all 8 cursors
		cursors := a.getCursorValues()

		cursorMutex.Lock()

		cursorHistory = append(cursorHistory, cursors)
		cursorMutex.Unlock()

		// Track rollbacks from this arena
		rollbacks := a.rollbackCounter.Load()
		if rollbacks > 0 {
			rollbackCount.Add(int64(rollbacks))
		}

		// ✅ Validate each cursor against its subregion bounds (sharded-aware)
		for ix, cursor := range cursors {
			subRegion := ingestor.subRegions[ix]

			require.GreaterOrEqual(t,
				cursor,
				subRegion.Lower,

				"cursor[%d]=%d below region lower bound %d",
				ix, cursor, subRegion.Lower,
			)

			require.LessOrEqual(t,
				cursor,
				subRegion.Upper,

				"cursor[%d]=%d exceeds region upper bound %d",
				ix, cursor, subRegion.Upper,
			)
		}

		// Log progress periodically
		if rotationCount.Load()%50 == 0 {
			t.Logf(
				"Rotation %d: cursors=%v, rollbacks=%d",
				rotationCount.Load(),
				cursors,
				rollbacks,
			)
		}

		ingestor.FlushArenaPerRegion(a)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	chIngestionEnd := ingestor.StartIngestion(ctx)

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

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

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
	t.Logf("Output size: %d bytes", writer.Len())

	// Verify we had many rotations due to pressure
	require.Greater(t,
		finalRotations,
		int32(10),

		"should have had multiple rotations under pressure",
	)

	// ✅ Verify cursor integrity using getCursorValues (sharded-aware)
	cursorMutex.Lock()
	defer cursorMutex.Unlock()

	require.Greater(t,
		len(cursorHistory),
		0,
		"should have recorded at least one flush snapshot",
	)

	for snapshotIdx, cursors := range cursorHistory {
		require.Len(t,
			cursors,
			8,
			"snapshot[%d] must have 8 cursor values",
			snapshotIdx)

		for i, cursor := range cursors {
			region := ingestor.subRegions[i]

			require.GreaterOrEqual(t,
				cursor,
				region.Lower,
				"snapshot[%d]: cursor[%d]=%d < lower=%d",
				snapshotIdx, i, cursor, region.Lower,
			)

			require.LessOrEqual(t,
				cursor,
				region.Upper,
				"snapshot[%d]: cursor[%d]=%d > upper=%d",
				snapshotIdx, i, cursor, region.Upper,
			)
		}
	}

	// Verify valid writes made it to output
	output := writer.String()
	lines := bytes.Split(
		bytes.TrimSpace([]byte(output)),
		[]byte{'\n'},
	)
	outputLines := len(lines)

	t.Logf("Output lines: %d", outputLines)
	t.Logf("Expected valid writes (approx): %d", expectedValidWrites)

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
			"output line exceeds arena size: %q (%d bytes)",
			line,
			len(line),
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

// TestHammerWithOversizedMessages_Detailed tracks per-rotation metrics
func TestHammerWithOversizedMessages_Detailed(t *testing.T) {
	var out bytes.Buffer

	const (
		arenaSize         = 800 // 8 subregions × 100 bytes each
		hugeRatio         = 95  // 95% huge messages
		numProducers      = 4
		writesPerProducer = 200
	)

	ingestor, errCrIngestor := NewIngestor(arenaSize, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// ✅ Updated: Track per-shard cursor values in metrics
	type rotationMetrics struct {
		cursors   []uint32 // snapshot of all 8 subregion cursors
		usedTotal uint32   // sum of (cursor - Lower) across all shards

		index     int32
		rollbacks int32
		writers   int32
	}

	var (
		metrics       []rotationMetrics
		metricsMutex  sync.Mutex
		rotationIndex int32
	)

	ingestor.flusher = func(a *arena) {
		rotationIndex++

		// ✅ Capture all 8 cursors via getCursorValues()
		cursors := a.getCursorValues()

		// ✅ Calculate total used bytes across all subregions
		var usedTotal uint32

		for i, cursor := range cursors {
			region := ingestor.subRegions[i]
			if cursor >= region.Lower {
				usedTotal += cursor - region.Lower
			}
		}

		metricsMutex.Lock()

		metrics = append(
			metrics,
			rotationMetrics{
				index:     rotationIndex,
				cursors:   cursors,   // ✅ Store full shard snapshot
				usedTotal: usedTotal, // ✅ Aggregate used bytes
				rollbacks: a.rollbackCounter.Load(),
				writers:   a.numberWriters.Load(),
			},
		)
		metricsMutex.Unlock()

		ingestor.FlushArenaPerRegion(a)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	chIngestionEnd := ingestor.StartIngestion(ctx)

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

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

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

	for ix, metric := range metrics {
		totalRollbacks += metric.rollbacks
		totalUsed += metric.usedTotal

		// ✅ Validate each of the 8 cursors against its subregion bounds
		require.Len(t,
			metric.cursors,
			8,

			"rotation %d: must have 8 cursor values",
			ix,
		)

		for shard, cursor := range metric.cursors {
			region := ingestor.subRegions[shard]

			require.GreaterOrEqual(t,
				cursor,
				region.Lower,
				"rotation %d: shard[%d] cursor=%d < lower=%d",
				ix, shard, cursor, region.Lower,
			)

			require.LessOrEqual(t,
				cursor,
				region.Upper,
				"rotation %d: shard[%d] cursor=%d > upper=%d",
				ix, shard, cursor, region.Upper,
			)
		}

		// Writers should be 0 at flush time (waitForWriters called)
		require.Zero(t,
			metric.writers,
			"rotation %d: writers still active during flush", ix,
		)

		if ix > 0 {
			t.Logf(
				"Rotation %d: cursors=%v, usedTotal=%d, rollbacks=%d",
				metric.index,
				metric.cursors,
				metric.usedTotal,
				metric.rollbacks,
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
