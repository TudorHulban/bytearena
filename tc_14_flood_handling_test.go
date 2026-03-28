package bytearena

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test Case 14: Exact Arena Size Edge Cases

// Test: Writes of exact arena size, writes larger than arena
// Verifies: Flood handling as described in Arena.md
func TestExactAndOversizedWrites(t *testing.T) {
	var sizeArena uint32 = 100

	ingestor, errCrIngestor := NewIngestor(sizeArena, &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Case 1: Write exactly arena size
	region, errWrite := ingestor.beginWrite(sizeArena)
	require.NoError(t, errWrite)
	require.Zero(t, region.offset)

	ingestor.EndWrite(region)

	require.EqualValues(t,
		sizeArena,
		ingestor.active.Load().cursor.Load(),
	)

	// Reset
	ingestor.rotate()

	// Case 2: Write larger than arena (flooding)
	region, errWrite = ingestor.beginWrite(sizeArena + 1)
	require.ErrorIs(t, errWrite, ErrWriteMessageTooLarge)
	require.Zero(t, region)

	// Rollback should not increment as there is oversize check.
	arenaActive := ingestor.active.Load()
	require.Zero(t,
		arenaActive.rollbackCounter.Load(),
	)

	// But cursor should NOT move
	require.Zero(t, arenaActive.cursor.Load())
}
