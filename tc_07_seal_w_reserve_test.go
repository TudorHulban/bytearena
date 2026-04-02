package bytearena

import (
	"bytes"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test Case: Race Between Reserve and Seal

// Test: Producer reserves space exactly as consumer seals
// Verifies: No writes to arena after it is sealed
func Test_1_ReserveVsSealRace(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(_Size1K, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Channel to coordinate race
	chReady := make(chan struct{})
	chDone := make(chan bool)

	// Producer goroutine
	go func() {
		<-chReady // Wait for signal

		// Attempt to reserve
		region, errWrite := ingestor.beginWrite(100)
		if errWrite == nil {
			// If we got a region, it must be in active arena
			if region.arena != ingestor.active.Load() && region.arena != nil {
				chDone <- false

				return
			}

			ingestor.EndWrite(region)
		}

		chDone <- true
	}()

	// Consumer goroutine
	go func() {
		<-chReady // Wait for same signal

		// Rotate arenas
		sealed := ingestor.rotate()
		_ = sealed
	}()

	// Start both simultaneously
	close(chReady)

	// Wait for result
	assert.True(t, <-chDone)

	// Verify invariant: No writes to sealed arena
	sealed := ingestor.sealed.Load()

	if sealed != nil {
		require.True(t,
			sealed.numberWriters.Load() == 0 || ingestor.active.Load() == sealed,
		)
	}
}

func Test_2_LateEnter_AfterDrainCheck(t *testing.T) {
	t.Skip()

	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(_Size1K, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// We will capture the arena being rotated out.
	original := ingestor.active.Load()
	require.NotNil(t, original)

	// Coordination
	chProducerMayEnter := make(chan struct{})
	chConsumerCheckedZero := make(chan struct{})
	chDone := make(chan struct{})

	// --- Producer ---
	go func() {
		// Step 1: read active arena early
		arena := ingestor.active.Load()
		require.Equal(t, original, arena)

		// Wait until consumer has already checked writers == 0
		<-chConsumerCheckedZero

		// Now enter AFTER drain check
		arena.Enter()

		// Signal we entered
		close(chProducerMayEnter)

		// Simulate some work
		runtime.Gosched()

		arena.Leave()
	}()

	// --- Consumer ---
	go func() {
		// Rotate: this seals `original`
		sealed := ingestor.rotate()
		require.Equal(t, original, sealed)

		// Spin until we observe zero writers
		for {
			if sealed.numberWriters.Load() == 0 {
				break
			}
			runtime.Gosched()
		}

		// Signal: "I believe it's drained"
		close(chConsumerCheckedZero)

		// Give producer a chance to enter AFTER this point
		<-chProducerMayEnter

		// Now check again
		if sealed.numberWriters.Load() != 0 {
			// ❌ This is the violation:
			// A writer appeared AFTER we observed zero
			t.Fatalf("late writer entered sealed arena after drain check")
		}

		close(chDone)
	}()

	select {
	case <-chDone:
	case <-time.After(2 * time.Second):
		t.Fatal("test timeout (possible deadlock)")
	}
}

func Test_3_DrainIsStable_UnderConcurrentEnter(t *testing.T) {
	var writer bytes.Buffer

	ingestor, err := NewIngestor(_Size1K, &writer)
	require.NoError(t, err)

	original := ingestor.active.Load()
	require.NotNil(t, original)

	var wgProducers sync.WaitGroup
	stop := make(chan struct{})

	// --- Producers hammer Enter/Leave ---
	for range 8 {
		wgProducers.Go(
			func() {
				for {
					select {
					case <-stop:
						return
					default:
					}

					arena := ingestor.active.Load()
					if arena == nil {
						continue
					}

					arena.Enter()
					runtime.Gosched() // widen race window
					arena.Leave()
				}
			},
		)
	}

	// --- Rotate once ---
	sealed := ingestor.rotate()
	require.Equal(t, original, sealed)

	// --- Try to observe a "false zero" ---
	var observedZero bool

	for range 100000 {
		if sealed.numberWriters.Load() == 0 {
			observedZero = true

			// Immediately re-check after yielding
			runtime.Gosched()

			if sealed.numberWriters.Load() != 0 {
				// ❌ This is the real violation
				close(stop)
				wgProducers.Wait()
				t.Fatalf(
					"unstable drain: observed 0 writers but new writer appeared",
				)
			}
		}
	}

	close(stop)
	wgProducers.Wait()

	// If we never even saw zero, that's also interesting
	require.True(t,
		observedZero,
		"never observed zero writers; test inconclusive",
	)
}

func Test_4_NoWriteAfterArenaReuse(t *testing.T) {
	var writer bytes.Buffer

	ingestor, err := NewIngestor(_Size1K, &writer)
	require.NoError(t, err)

	// Step 1: reserve a region but DO NOT write yet
	region, err := ingestor.beginWrite(64)
	require.NoError(t, err)

	arena := region.arena
	initialEpoch := arena.epoch.Load()

	// Step 2: force the arena to be rotated out and reused
	for i := 0; i < 3; i++ {
		_ = ingestor.rotate()
	}

	// At this point, arena may have been reset/reused
	newEpoch := arena.epoch.Load()

	// Sanity: ensure reuse actually happened
	if newEpoch == initialEpoch {
		t.Skip("arena was not reused; test inconclusive")
	}

	// Step 3: attempt to write using stale region
	defer ingestor.EndWrite(region)

	defer func() {
		if r := recover(); r != nil {
			// acceptable: defensive panic
			return
		}
	}()

	// This should NOT silently succeed if reuse occurred
	buf := region.Buf()
	for i := range buf {
		buf[i] = 0xFF
	}

	// If we reach here without panic or detection,
	// you potentially wrote into reused memory
	t.Fatalf("write succeeded on reused arena (epoch mismatch not enforced)")
}

func Test_5_NoWriteAfterArenaReuse_Offensive(t *testing.T) {
	var writer bytes.Buffer

	ingestor, err := NewIngestor(_Size1K, &writer)
	require.NoError(t, err)

	region, err := ingestor.beginWrite(64)
	require.NoError(t, err)

	arena := region.arena
	initialEpoch := arena.epoch.Load()

	var reused bool

	for range 1000 {
		a := ingestor.rotate()

		if a == arena && a.epoch.Load() != initialEpoch {
			reused = true
			break
		}
	}

	if !reused {
		t.Skip("arena was not reused (no epoch change observed)")
	}

	// Now THIS is real reuse
	defer ingestor.EndWrite(region)

	buf := region.Buf()
	for i := range buf {
		buf[i] = 0xCD
	}

	t.Fatalf("stale write succeeded after real reuse (epoch changed)")
}
