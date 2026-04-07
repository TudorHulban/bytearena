package bytearena

import (
	"bytes"
	"context"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// Test Case: Concurrent Writes During Rotation

// Test: Multiple producers writing while consumer rotates arenas
// Verifies: No writes are lost, no panics, all logs eventually appear
func TestConcurrentWrites(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(_Size1K, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	chIngestionEnd := ingestor.StartIngestion(ctx)

	var wgProducers sync.WaitGroup

	writes := 10000
	successCount := atomic.Int64{}

	noProducers := 10

	wgProducers.Add(noProducers)

	for ix := range noProducers {
		go func(id int) {
			defer wgProducers.Done()

			for j := 0; j < writes/noProducers; j++ {
				payload := helpers.SprintfInt(
					"producer-%d-%d\n",
					id,
					j,
				)

				if errWrite := ingestor.write(
					uint32(len(payload)),

					func(dst []byte) {
						copy(dst, payload)
					},
				); errWrite == nil {
					successCount.Add(1)
				}

				// Small random delay to increase race probability
				time.Sleep(time.Duration(rand.Intn(10)) * time.Microsecond)
			}
		}(ix)
	}

	wgProducers.Wait()
	cancel()

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

	output := writer.String()
	require.NotEmpty(t, output)

	require.NotZero(t, successCount.Load())

	t.Log(
		ingestor.Registry.Snapshot(),
	)

	// Verify: All successful writes appear in output
	outputNoLines := strings.Split(output, "\n")
	require.EqualValues(t,
		len(outputNoLines)-1,
		int(successCount.Load()),

		"number output lines: %d vs success count of %d",
		len(outputNoLines),
		int(successCount.Load()),
	)
}
