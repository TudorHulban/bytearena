package bytearena

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

func TestErrorWriters(t *testing.T) {
	testCases := []struct {
		writer      io.Writer
		description string
	}{
		{
			description: "1. Random Fails",
			writer:      helpers.NewRandomFailWriter(2),
		},
		{
			description: "2. Partial Writes",
			writer: helpers.NewPartialWriter(
				5,
				3,
				5,
			),
		},
		{
			description: "3. Slow after some writes",
			writer: helpers.NewSlowAfterWriter(
				5,
				3,
				5,
			),
		},
		{
			description: "4. Error after some writes",
			writer:      helpers.NewErrorAfterWriter(),
		},
		{
			description: "5. Zero Progress Writer",
			writer:      &helpers.ZeroProgressWriter{},
		},
		{
			description: "6. Closing writer",
			writer:      helpers.NewClosingWriter(2),
		},
	}

	for _, tc := range testCases {
		t.Run(
			tc.description,
			func(t *testing.T) {
				ingestor, errCrIngestor := NewIngestor(
					_Size1K,
					tc.writer,
				)
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

				require.NotZero(t, successCount.Load())

				t.Log(
					ingestor.Registry.Snapshot(),
				)

				require.GreaterOrEqual(t,
					int64(totalWrites),
					successCount.Load()-int64(len(ingestor.Registry.Snapshot())),
				)
			},
		)
	}
}
