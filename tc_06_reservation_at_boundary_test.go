package bytearena

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test Case: Reservation When All Subregions Are Full
//
// Test: Fill all 8 subregions to their Upper bounds (100 bytes each = 800 total),
// then attempt a 1-byte write.
//
// Verifies:
// - Each subregion cursor respects its Lower/Upper bounds (no off-by-one)
// - When all shards are full, beginWrite(1) returns ErrWriteArenaFull
// - Failed reservation increments rollbackCounter for observability
//
// Note: Ingestion consumer loop is not started, preventing automatic rotation.
// This ensures the boundary condition is tested deterministically on a single arena.
func TestReservationAtBoundary(t *testing.T) {
	var writer bytes.Buffer
	const arenaSize = 800 // 8 subregions × 100 bytes each

	ingestor, errCrIngestor := NewIngestor(arenaSize, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Verify subregion layout
	subRegions := ingestor.subRegions
	require.Len(t, subRegions, 8)

	for i, subRegion := range subRegions {
		expectedLower := uint32(i) * 100
		expectedUpper := uint32(i+1) * 100

		if i == 7 {
			expectedUpper = arenaSize // last region absorbs remainder
		}
		require.Equal(t, expectedLower, subRegion.Lower, "region[%d].Lower", i)
		require.Equal(t, expectedUpper, subRegion.Upper, "region[%d].Upper", i)
	}

	arena := ingestor.active.Load()

	// Initial state: all cursors at their region's Lower bound
	initialCursors := arena.getCursorValues()

	for i := range initialCursors {
		require.Equal(t,
			uint64(subRegions[i].Lower),
			initialCursors[i],

			"initial cursor[%d] should be at region Lower bound",
			i,
		)
	}

	// Fill each subregion with a 100-byte write (8 writes × 100 bytes = 800 bytes total)
	for i := range 8 {
		region, errWrite := ingestor.beginWrite(100)
		require.NoError(t, errWrite, "write %d of 100 bytes should succeed", i)
		require.NotNil(t, region)

		// Complete the write to release writer slot
		ingestor.EndWrite(region)
	}

	// Verify all subregion cursors advanced to their Upper bounds
	cursors := arena.getCursorValues()
	for i, cursor := range cursors {
		region := subRegions[i]

		require.Equal(t,
			uint64(region.Upper),
			cursor,

			"cursor[%d] should be at Upper bound after 100-byte write",
			i,
		)
	}

	// Now try to write 1 more byte - arena is 100% full, should fail
	regionExtra, errExtra := ingestor.beginWrite(1)

	// Expect failure: arena physically cannot accept more data
	require.ErrorIs(t,
		errExtra,
		ErrWriteSubRegionFull,

		"after filling all 800 bytes, 1-byte write should fail with ErrWriteArenaFull",
	)
	require.Zero(t, regionExtra, "failed write should return zero region")

	// Verify rollback was recorded for the failed reservation attempt
	require.Equal(t,
		int32(1),
		arena.rollbackCounter.Load(),

		"failed write should increment rollback counter")

	// Epochs remain zero: rotation is triggered in consumer loop, not during beginWrite failure
	e1, e2 := ingestor.GetArenaEpochs()
	require.Zero(t,
		e1,
		"epoch should not advance without consumer rotation",
	)
	require.Zero(t,
		e2,
		"epoch should not advance without consumer rotation",
	)
}
