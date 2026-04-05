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

// Test_ManyRotations_02_CursorIntegrity specifically tests cursor
// remains within bounds during many rotations.
func Test_ManyRotations_02_CursorIntegrity(t *testing.T) {
	var writer bytes.Buffer

	const (
		arenaSize    = 256
		numRotations = 500
	)

	ingestor, errCrIngestor := NewIngestor(arenaSize, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Track cursor values across rotations for all 8 subregions
	var (
		cursorHistory [][]uint64 // each entry: snapshot of 8 cursor values at flush time
		cursorMutex   sync.Mutex
	)

	ingestor.flusher = func(a *arena) {
		cursorMutex.Lock()

		// Capture snapshot of all 8 cursors
		cursorHistory = append(cursorHistory, a.getCursorValues())
		cursorMutex.Unlock()

		// Validate each cursor is within its subregion bounds
		cursors := a.getCursorValues()

		for ix, cursor := range cursors {
			subRegion := ingestor.subRegions[ix]

			require.GreaterOrEqual(t,
				cursor,
				uint64(subRegion.Lower),

				"cursor[%d] value %d below region lower bound %d",
				ix,
				cursor,
				subRegion.Lower,
			)

			require.LessOrEqual(t,
				cursor,
				uint64(subRegion.Upper),

				"cursor[%d] value %d exceeds region upper bound %d",
				ix,
				cursor,
				subRegion.Upper,
			)
		}

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

	// === NEW: Verify all subregions were actively used ===
	cursorMutex.Lock()
	defer cursorMutex.Unlock()

	require.Greater(t,
		len(cursorHistory),
		0,
		"should have recorded at least one flush",
	)

	// Track which subregions showed cursor movement beyond initial Lower bound
	regionUsed := make([]bool, 8)
	initialCursors := make([]uint64, 8)

	for i := range initialCursors {
		initialCursors[i] = uint64(ingestor.subRegions[i].Lower)
	}

	for _, snapshot := range cursorHistory {
		require.Len(t,
			snapshot,
			8,
			"each snapshot must have 8 cursor values",
		)

		for i, cursor := range snapshot {
			if cursor > initialCursors[i] {
				regionUsed[i] = true
			}
		}
	}

	// Verify all 8 subregions had at least one cursor advance
	for i, used := range regionUsed {
		require.True(t,
			used,

			"subregion[%d] (bounds [%d,%d]) cursor never advanced from initial value %d — shard may be unused",
			i,
			ingestor.subRegions[i].Lower,
			ingestor.subRegions[i].Upper,
			initialCursors[i],
		)
	}

	// Optional: log coverage for visibility
	t.Logf("Subregion usage: %v", regionUsed)
	t.Logf("Total flush snapshots: %d", len(cursorHistory))
}
