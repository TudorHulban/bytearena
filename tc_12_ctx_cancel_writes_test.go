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
func TestContextCancel_DuringHeavyWrite(t *testing.T) {
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
		flushedBytes.Add(int64(a.cursor.Load()))

		// Log progress periodically
		if rotations%100 == 0 {
			t.Logf(
				"Rotation %d: flushed %d bytes",
				rotations,
				a.cursor.Load(),
			)
		}

		ingestor.flushArena(a)

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
	for p := range numProducers {
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
		}(p)
	}

	// Wait for either:
	// - target rotations reached (signaled by doneChan)
	// - timeout (safety)
	select {
	case <-chDone:
		t.Log("Target rotations reached, shutdown initiated")

	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for target rotations")
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

// TestContextCancelDuringRotation specifically tests cancellation
// during the rotation process itself.
func TestContextCancel_DuringRotation(t *testing.T) {
	var out bytes.Buffer

	const arenaSize = 1024

	ingestor, errCrIngestor := NewIngestor(arenaSize, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Track rotation phases
	var (
		rotationStarted   atomic.Bool
		rotationCompleted atomic.Bool
		flusherCalled     atomic.Bool
	)

	ingestor.flusher = func(a *arena) {
		rotationStarted.Store(true)
		flusherCalled.Store(true)

		t.Logf(
			"Flusher called with %d bytes",
			a.cursor.Load(),
		)

		// Simulate slow flush
		time.Sleep(50 * time.Millisecond)

		ingestor.flushArena(a)
		a.reset()

		rotationCompleted.Store(true)
	}

	// Create context that will be cancelled mid-rotation
	ctx, cancel := context.WithCancel(context.Background())

	var wgConsumer sync.WaitGroup

	// Start consumer with instrumentation
	wgConsumer.Go(
		func() {
			ingestor.consumerLoop(ctx)
		},
	)

	// Start a producer that holds a write open
	region, errWrite := ingestor.beginWrite(500)
	require.NoError(t, errWrite)

	// Don't endWrite yet - producer is "stuck"

	// Wait for consumer to start monitoring
	time.Sleep(10 * time.Millisecond)

	// Cancel context while rotation is happening
	cancel()

	// Wait for consumer to exit
	chDone := make(chan struct{})

	go func() {
		wgConsumer.Wait()
		close(chDone)
	}()

	select {
	case <-chDone:
		t.Log(
			"Consumer exited cleanly",
		)

	case <-time.After(200 * time.Millisecond):
		t.Fatal(
			"Consumer did not exit after context cancel",
		)
	}

	// Complete the stuck write
	ingestor.EndWrite(region)

	// Verify final state
	require.True(t,
		rotationCompleted.Load(),
		"Rotation is done anyway before seal.",
		"Shutdown flush should have completed (best-effort, ctx-aware)",
	)

	// The consumer exited before the stuck writer completed,
	// but best-effort flush on shutdown still ran.
	require.True(t,
		flusherCalled.Load(),
		"Flusher should have been called on shutdown (best-effort)",
	)

	// Verify arenas are in consistent state
	activeArena := ingestor.active.Load()

	// Active arena is niled during shutdown.
	require.Nil(t, activeArena)
}

// TestContextCancelWithPendingWrites tests cancellation while
// there are pending writes in both arenas
func TestContextCancel_WithPendingWrites(t *testing.T) {
	var out bytes.Buffer

	const arenaSize = 1024 // large enough to hold all pending writes comfortably

	ingestor, errCrIngestor := NewIngestor(arenaSize, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	flusherCallCount := atomic.Int32{}

	ingestor.flusher = func(a *arena) {
		flusherCallCount.Add(1)

		t.Logf(
			"Flusher called for arena with %d bytes",
			a.cursor.Load(),
		)

		ingestor.flushArena(a)
		a.reset()
	}

	ctx, cancel := context.WithCancel(context.Background())

	noRegions := 5

	// Grab write regions in arena1 without releasing them yet —
	// simulates in-flight producers that haven't called endWrite.
	regions := make([]WriteRegion, 0, noRegions+3+3)

	for i := range noRegions {
		region, errWrite := ingestor.beginWrite(50)
		require.NoError(t, errWrite)

		regions = append(regions, region)
		copy(
			region.Buf(),
			[]byte(fmt.Sprintf("pending-write-%d", i)),
		)
	}

	// Rotate: arena1 is now sealed with 5 unreleased writers.
	sealed := ingestor.rotate()
	require.Equal(t, ingestor.arenaFirst, sealed)

	// Grab 3 more write regions in arena2, also unreleased.
	arena2 := ingestor.active.Load()
	require.NotEqual(t, sealed, arena2)

	for i := range 3 {
		region, err := ingestor.beginWrite(50)
		require.NoError(t, err)

		regions = append(regions, region)
		copy(
			region.Buf(),
			[]byte(fmt.Sprintf("second-arena-%d", i)),
		)
	}

	for i := range 3 {
		region, errWrite := ingestor.beginWrite(50)
		require.NoError(t, errWrite)

		regions = append(regions, region)
		copy(
			region.Buf(),
			[]byte(fmt.Sprintf("second-arena-%d", i)),
		)
	}

	// Start consumer that will be cancelled
	var wgConsumer sync.WaitGroup

	wgConsumer.Go(
		func() {
			ingestor.consumerLoop(ctx)
		},
	)

	// Give consumer time to start
	time.Sleep(10 * time.Millisecond)

	// Cancel context while writes are pending in both arenas
	cancel()

	// Wait for consumer to exit
	chDome := make(chan struct{})

	go func() {
		wgConsumer.Wait()
		close(chDome)
	}()

	select {
	case <-chDome:
		t.Log(
			"Consumer exited cleanly",
		)

	case <-time.After(500 * time.Millisecond):
		t.Fatal(
			"Consumer did not exit",
		)
	}

	// Complete all pending writes
	for _, region := range regions {
		ingestor.EndWrite(region)
	}

	// Verify final state
	require.Greater(t,
		flusherCallCount.Load(),
		int32(0),
		"Flusher should have been called at least once",
	)

	// Verify all arenas have zero writers
	require.Zero(t,
		ingestor.arenaFirst.numberWriters.Load(),
		"First arena writers leaked",
	)
	require.Zero(t,
		ingestor.arenaSecond.numberWriters.Load(),
		"Second arena writers leaked",
	)

	// Output should contain all writes
	output := out.String()

	for i := range 5 {
		require.Contains(t,
			output,
			fmt.Sprintf("pending-write-%d", i),
		)
	}

	for i := range 3 {
		require.Contains(t,
			output,
			fmt.Sprintf("second-arena-%d", i),
		)
	}
}
