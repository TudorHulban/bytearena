package bytearena

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test Case: Reservation Exactly at Arena Boundary

// Test: Producer reserves last bytes exactly at arenaSize-1
// Verifies:
// - bounds checking works, no off-by-one errors
// - rollback counter incremented
func TestReservationAtBoundary(t *testing.T) {
	ingestor, errCrIngestor := NewIngestor(100, &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	arena := ingestor.active.Load()

	// Fill arena to 90 bytes
	arena.cursor.Store(90)

	// Producer 1: Reserve 10 bytes (should fit exactly)
	region10, errReserve10 := ingestor.beginWrite(10)
	require.NoError(t, errReserve10)
	require.EqualValues(t,
		90,
		region10.offset,
	)

	// Producer 2: Reserve 1 byte (should fail - overflow)
	regionZero, errReserveMore := ingestor.beginWrite(1)
	require.ErrorIs(t,
		errReserveMore,
		ErrWriteArenaFull,
	)
	require.Zero(t, regionZero)
	require.EqualValues(t,
		1,
		arena.rollbackCounter.Load(),
	)

	// Complete first write
	ingestor.EndWrite(region10)

	// Verify: Final cursor at 100
	require.EqualValues(t,
		100,
		arena.cursor.Load(),
	)

	e1, e2 := ingestor.GetArenaEpochs()
	require.Zero(t, e1)
	require.Zero(t, e2)
}
