package bytearena

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test Case: Overflow and Rollback Storm (Sharded Subregions)
//
// Test: Many producers simultaneously attempt writes when subregions are near capacity.
// Verifies:
// - Rollback counter correctly tracks failed reservations across all shards
// - Each subregion cursor stays within its Lower/Upper bounds (no off-by-one)
// - No deadlocks under high contention
//
// Note: Ingestion is not started, so no automatic rotation occurs.
// This ensures we test boundary conditions deterministically on active arena.
func TestRollbackStorm(t *testing.T) {
	const arenaSize = _Size1K // 1024 bytes → 8 subregions × 128 bytes each

	ingestor, errCrIngestor := NewIngestor(arenaSize, &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	arena := ingestor.active.Load()

	// Fill each subregion near its upper bound (leave small headroom for occasional success)
	// Region size = 1024/8 = 128 bytes; set cursors to Lower + 118 (10 bytes headroom)
	headroom := uint32(10)

	for ix := range ingestor.subRegions {
		region := ingestor.subRegions[ix]
		targetCursor := region.Upper - headroom

		if targetCursor < region.Lower {
			targetCursor = region.Lower // Safety: do not underflow
		}

		arena.subRegionCursors[ix].value.Store(targetCursor)
	}

	var wgProducers sync.WaitGroup

	rollbacks := atomic.Int64{}
	successes := atomic.Int64{}

	noProducers := 100
	wgProducers.Add(noProducers)

	// Concurrent producers each trying to write varying sizes
	for range noProducers {
		go func() {
			defer wgProducers.Done()

			for range 10 {
				// Random size between 10-100 bytes
				size := uint32(10 + rand.Intn(90))

				region, errWrite := ingestor.beginWrite(size)
				if errWrite == nil {
					successes.Add(1)
					ingestor.endWrite(region)
				} else {
					rollbacks.Add(1)
				}
			}
		}()
	}

	wgProducers.Wait()

	// Verify: Rollback counter matches failures
	require.EqualValues(t,
		rollbacks.Load(),
		arena.rollbackCounter.Load(),

		"rollback counter should match observed failures",
	)

	// Verify at least some activity occurred
	require.True(t,
		successes.Load() > 0 || rollbacks.Load() > 0,

		"expected at least one success or rollback",
	)

	// Verify all subregion cursors stayed within bounds
	cursors := arena.getCursorValues()

	for ix, cursor := range cursors {
		region := ingestor.subRegions[ix]

		require.GreaterOrEqual(t,
			cursor,
			region.Lower,

			"cursor[%d]=%d below lower bound %d",
			ix,
			cursor,
			region.Lower,
		)

		require.LessOrEqual(t,
			cursor,
			region.Upper,

			"cursor[%d]=%d exceeds upper bound %d",
			ix,
			cursor,
			region.Upper,
		)
	}

	// Optional: log summary for visibility
	t.Logf(
		"Successes: %d, Rollbacks: %d, Counter: %d",
		successes.Load(),
		rollbacks.Load(),
		arena.rollbackCounter.Load(),
	)

	t.Logf("Final cursors: %v", cursors)
}
