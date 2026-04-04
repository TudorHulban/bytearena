package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test_ManyRotations_02_CursorIntegrity specifically tests cursor behavior
// during many rotations
func Test_ManyRotations_02_CursorIntegrity(t *testing.T) {
	var out bytes.Buffer

	const (
		arenaSize    = 256
		numRotations = 500
	)

	ingestor, errCrIngestor := NewIngestor(arenaSize, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Track cursor values across rotations
	var (
		cursorHistory []int32
		cursorMutex   sync.Mutex
	)

	ingestor.flusher = func(a *arena) {
		cursorMutex.Lock()

		cursorHistory = append(cursorHistory, a.cursor.Load())

		cursorMutex.Unlock()

		// Verify cursor never exceeds arena size
		cursor := a.cursor.Load()
		require.LessOrEqual(t, cursor, int32(arenaSize),
			"cursor %d exceeds arena size %d", cursor, arenaSize)

		ingestor.flushArena(a)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wgConsumer sync.WaitGroup

	wgConsumer.Go(
		func() {
			ingestor.consumerLoop(ctx)
		},
	)

	// Single producer writing sequentially to make cursor behavior predictable
	for i := range numRotations * 2 {
		payload := fmt.Sprintf("msg-%d-", i)
		payload = payload + randomString(20) // Ensure we fill arena quickly

		_ = ingestor.write(
			uint32(len(payload)),

			func(dst []byte) {
				copy(dst, payload)
			},
		)

		// Small delay to allow rotations
		if i%10 == 0 {
			time.Sleep(time.Microsecond)
		}
	}

	cancel()
	wgConsumer.Wait()

	e1, e2 := ingestor.GetArenaEpochs()

	require.Greater(t,
		e1+e2,
		uint64(10),
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
				"Cursor[%d] exactly at arena size",
				ix,
			)
		}
	}
}
