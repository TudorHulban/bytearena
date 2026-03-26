package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test Case 03: Concurrent Writes During Rotation

// Test: Multiple producers writing while consumer rotates arenas
// Verifies: No writes are lost, no panics, all logs eventually appear
func TestConcurrentWritesWithRotation(t *testing.T) {
	var out bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(_Size1K, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var wgConsumer sync.WaitGroup

	// Start consumer with aggressive rotation
	wgConsumer.Go(
		func() {
			ingestor.consumerLoop(ctx)
		},
	)

	var wgProducers sync.WaitGroup

	writes := 10000
	successCount := atomic.Int64{}

	noProducers := 10

	wgProducers.Add(noProducers)

	for ix := range noProducers {
		go func(id int) {
			defer wgProducers.Done()

			for j := 0; j < writes/noProducers; j++ {
				payload := fmt.Sprintf(
					"producer-%d-%d\n",
					id,
					j,
				)

				if errWrite := ingestor.write(
					uint32(len(payload)),

					func(dst []byte) {
						copy(dst, []byte(payload))
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

	wgConsumer.Wait()

	// Verify: All successful writes appear in output
	output := out.String()
	require.NotEmpty(t, output)

	require.NotZero(t, successCount.Load())

	outputNoLines := strings.Split(output, "\n")
	require.EqualValues(t,
		len(outputNoLines)-1,
		int(successCount.Load()),

		"number output lines: %d vs success count of %d",
		len(outputNoLines),
		int(successCount.Load()),
	)
}
