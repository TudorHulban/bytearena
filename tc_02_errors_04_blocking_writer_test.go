package bytearena

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// Test Case: Backpressure policy - Blocking Writer

// Test: Writer stops indefinitely after n writes.
// Verifies logger enters full mode
// with silently drop (common for high-perf logging).
// No error reported to the producers.
// No blocking.

func TestConcurrentWrites_BlockingWriter(t *testing.T) {
	var legitWrites uint64 = 10

	writer := helpers.NewBlockingWriter(legitWrites)

	ingestor, errCrIngestor := NewIngestor(1024, writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)

	chIngestionEnd := ingestor.StartIngestion(ctx)

	var wgProducers sync.WaitGroup

	totalWrites := 1000
	successCount := atomic.Int64{}

	noProducers := 10

	wgProducers.Add(noProducers)

	for ix := range noProducers {
		go func(id int) {
			defer wgProducers.Done()

			for j := 0; j < totalWrites/noProducers; j++ {
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

	output := writer.Buf.String()
	require.NotEmpty(t, output)

	require.NotZero(t, successCount.Load())

	t.Log(
		ingestor.Registry.Snapshot(),
	)

	require.EqualValues(t,
		totalWrites,
		successCount.Load(),
	)
}
