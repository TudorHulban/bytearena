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

// Test Case: Context cancel during heavy write
//
// Test: Multiple rotates. Say 1000.
// Verifies: After multiple rotations,
// a context cancellation flushes correctly.
func Test_1_ContextCancel_DuringHeavyWrite(t *testing.T) {
	var out bytes.Buffer

	// Use small arena to force frequent rotations
	const (
		arenaSize       = 256 // bytes
		targetRotations = 1000
		numProducers    = 12
	)

	ingestor, errCrIngestor := NewIngestor(arenaSize, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Track metrics
	var (
		rotationCount   atomic.Int32
		writesAttempted atomic.Int64
		writesSucceeded atomic.Int64
		flushedBytes    atomic.Int64
		shutdownStarted atomic.Bool
	)

	// Channel to signal when we have hit target rotations.
	chDone := make(chan struct{})

	// Create a cancellable context for the consumer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ingestor.flusher = func(a *arena) {
		// Count this rotation
		rotations := rotationCount.Add(1)

		// Track flushed bytes
		flushedBytes.Add(
			int64(
				ingestor.getArenaData(a).Len(),
			),
		)

		e1, e2 := ingestor.GetArenaEpochs()

		// Log progress periodically
		if rotations%100 == 0 {
			t.Logf(
				"Epochs %d: flushed %d bytes",
				rotations,
				e1+e2,
			)
		}

		ingestor.flushArenaPerRegion(a)

		// When we hit target rotations, trigger shutdown
		if rotations >= targetRotations && !shutdownStarted.Load() {
			shutdownStarted.Store(true)
			t.Logf(
				"Target rotations (%d) reached, initiating shutdown",
				targetRotations,
			)

			close(chDone)
			cancel() // Cancel context to trigger shutdown
		}
	}

	var wgConsumer sync.WaitGroup

	// Start consumer with rotation tracking
	wgConsumer.Go(
		func() {
			ingestor.consumerLoop(ctx)

			t.Log("Consumer loop exited")
		},
	)

	// Wait for consumer to be ready
	time.Sleep(10 * time.Millisecond)

	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	// Start producers that will keep writing until context is done
	for ix := range numProducers {
		go func(producerID int) {
			defer wgProducers.Done()

			// Each producer gets its own rand source to avoid contention
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(producerID)))

			writeCount := 0

			for {
				// Check if we should stop
				select {
				case <-ctx.Done():
					t.Logf(
						"Producer %d stopping after %d writes (context done)",
						producerID,
						writeCount,
					)

					return

				default:
				}

				// Random payload size between 10-80 bytes to create pressure
				size := 10 + r.Intn(70)

				payload := fmt.Sprintf(
					"p%d-%d-%s\n",
					producerID,
					writeCount,
					randomString(size-9), // Adjust for prefix
				)

				writesAttempted.Add(1)

				errWrite := ingestor.write(
					uint32(len(payload)),

					func(destination []byte) {
						copy(destination, payload)
					},
				)
				if errWrite == nil {
					writesSucceeded.Add(1)
				}

				writeCount++

				// Small random delay to increase race probability
				if writeCount%5 == 0 {
					time.Sleep(time.Duration(r.Intn(10)) * time.Microsecond)
				}
			}
		}(ix)
	}

	// Wait for either:
	// - target rotations reached (signaled by doneChan)
	// - timeout (safety)
	select {
	case <-chDone:
		t.Log(
			"Target rotations reached, shutdown initiated",
		)

	case <-time.After(10 * time.Second):
		t.Fatal(
			"Timeout waiting for target rotations",
		)
	}

	// Give time for shutdown to complete
	shutdownStart := time.Now()

	// Wait for consumer to finish (with timeout)
	chConsumerDone := make(chan struct{})

	go func() {
		wgConsumer.Wait()
		close(chConsumerDone)
	}()

	select {
	case <-chConsumerDone:
		shutdownDuration := time.Since(shutdownStart)

		t.Logf(
			"Clean shutdown completed in %v",
			shutdownDuration,
		)

	case <-time.After(2 * time.Second):
		// Force cancel again
		cancel()
		<-chConsumerDone

		t.Log(
			"Shutdown completed after forced cancel",
		)
	}

	wgProducers.Wait()
	t.Log("All producers stopped")

	// Collect final metrics
	finalRotations := rotationCount.Load()
	finalAttempted := writesAttempted.Load()
	finalSucceeded := writesSucceeded.Load()
	finalFlushed := flushedBytes.Load()

	t.Log("=== Final Metrics ===")
	t.Logf("Rotations: %d", finalRotations)
	t.Logf("Writes attempted: %d", finalAttempted)
	t.Logf("Writes succeeded: %d", finalSucceeded)
	t.Logf("Flushed bytes: %d", finalFlushed)
	t.Logf("Output size: %d bytes", out.Len())

	// Verify we had many rotations
	require.GreaterOrEqual(t,
		finalRotations,
		int32(targetRotations),

		"Should have achieved at least %d rotations",
		targetRotations,
	)

	// Verify output integrity
	output := out.Bytes()
	lines := bytes.Split(bytes.TrimSpace(output), []byte{'\n'})

	// Count actual lines (excluding empty last line if present)
	outputLines := len(lines)

	// All successful writes are not guaranteed to be in output.
	// Writes that completed after flushOnShutdown are not guaranteed to be flushed.
	// Allow a small delta proportional to the number of producers.
	require.GreaterOrEqual(t,
		outputLines,
		int(finalSucceeded)-numProducers,

		"Too many writes lost: output=%d succeeded=%d",
		outputLines,
		finalSucceeded,
	)

	require.LessOrEqual(t,
		outputLines,
		int(finalSucceeded),

		"More lines than writes: output=%d succeeded=%d",
		outputLines,
		finalSucceeded,
	)

	// Verify no partial writes in output
	for i, line := range lines {
		if len(line) == 0 {
			continue
		}

		// Each line should end with newline in the original output
		require.Regexp(t,
			`^p\d+-\d+-[a-z]+$`,
			string(line),

			"Line %d has invalid format: %q",
			i,
			line,
		)
	}
}

// TestContextCancelWithPendingWrites tests cancellation while
// there are pending writes in both arenas.
func Test_ContextCancel_WithPendingWrites_Deterministic(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(1024, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Track flush calls
	flusherCallCount := atomic.Int32{}
	originalFlusher := ingestor.flusher

	ingestor.flusher = func(a *arena) {
		flusherCallCount.Add(1)

		_, total := a.getSubregionLoads()

		t.Logf(
			"Flusher called with %d bytes",
			total,
		)

		originalFlusher(a) // Delegate to real flush logic
		// Do NOT call a.reset() here — production code handles lifecycle
	}

	// Synchronization: signal when consumer is ready
	consumerReady := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	var wgConsumer sync.WaitGroup

	wgConsumer.Go(
		func() {
			close(consumerReady) // Signal ready
			ingestor.consumerLoop(ctx)
		},
	)

	<-consumerReady // Wait for consumer to start

	// Write pending data to arena1 (5 writers)
	regions := make([]writeRegion, 0, 11)

	for i := range 5 {
		region, err := ingestor.beginWrite(50)
		require.NoError(t, err)

		regions = append(regions, region)

		copy(
			region.Buf(),
			[]byte(
				fmt.Sprintf("pending-write-%d", i),
			),
		)
	}

	// Force rotation by writing enough to trigger shouldSeal()
	// Or manually rotate if testing primitive (document this)
	sealed := ingestor.rotate()
	require.NotNil(t, sealed)

	// Write to arena2 (6 more writers)
	for i := range 6 {
		region, err := ingestor.beginWrite(50)
		require.NoError(t, err)

		regions = append(regions, region)
		copy(
			region.Buf(),
			[]byte(
				fmt.Sprintf("arena2-write-%d", i),
			),
		)
	}

	// ✅ Complete ALL writes BEFORE cancellation to ensure flush can proceed
	for _, region := range regions {
		ingestor.EndWrite(region)
	}

	// Small delay to let tick() observe sealed arena
	time.Sleep(20 * time.Millisecond)

	// Now cancel and wait for clean shutdown
	cancel()

	done := make(chan struct{})

	go func() {
		wgConsumer.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Consumer exited cleanly")

	case <-time.After(500 * time.Millisecond):
		t.Fatal("Consumer did not exit in time")
	}

	// ✅ Assert flusher was called
	require.Greater(t,
		flusherCallCount.Load(),
		int32(0),

		"Flusher should have been called at least once",
	)

	// ✅ Verify output contains expected data (only if flush succeeded)
	output := writer.String()

	// Note: If timeout caused flush skip, these may fail — that's expected
	// For deterministic test, ensure writers finish before timeout
	for i := range 5 {
		require.Contains(t,
			output,
			fmt.Sprintf("pending-write-%d", i),
		)
	}

	for i := range 6 {
		require.Contains(t,
			output,
			fmt.Sprintf("arena2-write-%d", i),
		)
	}

	// ✅ Verify no writer leaks
	require.Zero(t, ingestor.arenaFirst.numberWriters.Load())
	require.Zero(t, ingestor.arenaSecond.numberWriters.Load())
}
