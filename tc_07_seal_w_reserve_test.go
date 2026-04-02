package bytearena

import (
	"bytes"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test Case: Race Between Reserve and Seal

func Test_1_DrainIsStable_UnderConcurrentEnter(t *testing.T) {
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

func Test_2_NoWriteAfterArenaReuse(t *testing.T) {
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

func Test_3_NoWriteAfterArenaReuse_Offensive(t *testing.T) {
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
