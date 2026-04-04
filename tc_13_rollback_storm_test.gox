package bytearena

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test Case: Overflow and Rollback Storm

// Test: Many producers simultaneously attempt writes near arena end.
// Verifies: Rollback counter correctly tracks failures, no deadlocks.
func TestRollbackStorm(t *testing.T) {
	ingestor, errCrIngestor := NewIngestor(_Size1K, &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	arena := ingestor.active.Load()

	// Fill arena near capacity
	arena.cursor.Store(950)

	var wgProducers sync.WaitGroup

	rollbacks := atomic.Int64{}
	successes := atomic.Int64{}

	noProducers := 100

	wgProducers.Add(noProducers)

	// concurrent producers each trying to write varying sizes
	for range noProducers {
		go func() {
			defer wgProducers.Done()

			for range 10 {
				// Random size between 10-100 bytes
				size := uint32(10 + rand.Intn(90))

				region, errWrite := ingestor.beginWrite(size)
				if errWrite == nil {
					successes.Add(1)
					ingestor.EndWrite(region)
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
	)
	require.True(t, successes.Load() > 0 || rollbacks.Load() > 0)
	require.True(t, arena.cursor.Load() <= _Size1K) // Never exceed
}
