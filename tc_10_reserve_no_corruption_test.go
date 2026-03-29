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
	"github.com/tudorhulban/bytearena/helpers"
)

// Test Case: Data corruption risk in Reserve rollback
//
// Test scenario:
// Arena size is 1000 bytes.
// Simulate high contention where multiple producers attempt to reserve space
// near the end of the arena, triggering rollbacks. Verify that:
// 1. No data corruption occurs (no overlapping writes)
// 2. Rollback counter accurately reflects failed reservations
// 3. All successful writes are correctly preserved
// 4. No panic or race conditions occur
func TestReserveNoCorruption(t *testing.T) {
	arenaSize := uint32(1000)
	writer := bytes.Buffer{}

	// Configure aggressive seal percentage to trigger rotation early
	// This forces the arena to seal and reset frequently, exposing rollback issues.
	ingestor, errCrIngestor := NewIngestor(
		arenaSize,
		&writer,
		WithSealPercentage(30),
		WithTelemetry(),
	)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	chIngestionEnd := ingestor.StartIngestion(ctx)

	var wgProducers sync.WaitGroup

	// Track successful writes and their expected content
	successCount := atomic.Int64{}
	expectedMessages := sync.Map{}

	noProducers := 30
	writesPerProducer := 300

	wgProducers.Add(noProducers)

	// Each producer writes messages of varying sizes to create fragmentation.
	for ix := range noProducers {
		go func(producerID int) {
			defer wgProducers.Done()

			for w := range writesPerProducer {
				// Variable message sizes (5-50 bytes) to increase fragmentation
				// and probability of edge-case reservations
				msgSize := 5 + rand.Intn(45)

				payload := fmt.Sprintf(
					"p%d-w%d-%s\n",
					producerID,
					w,
					randomString(msgSize),
				)

				errWrite := ingestor.write(
					uint32(len(payload)),

					func(dst []byte) {
						copy(dst, payload)
					},
				)
				if errWrite == nil {
					successCount.Add(1)

					expectedMessages.Store(payload, true) // Store expected content for verification
				}

				// Random delay to increase race probability
				time.Sleep(time.Duration(rand.Intn(5)) * time.Microsecond)
			}
		}(ix)
	}

	wgProducers.Wait()

	// Give consumer time to flush remaining arenas
	time.Sleep(50 * time.Millisecond)

	cancel()
	<-chIngestionEnd

	require.Greater(t,
		ingestor.Metrics.NumberRollbacks.Load(),
		uint64(0),
	)
	fmt.Printf(
		"Total message rollbacks: %d.\n",
		ingestor.Metrics.NumberRollbacks.Load(),
	)

	// Verify: All successful writes appear exactly once in output
	output := writer.String()
	require.NotEmpty(t, output)

	// Parse output lines and verify no corruption
	outputLines := strings.Split(
		strings.TrimSuffix(output, "\n"), "\n",
	)

	// Count occurrences of each message
	messageCounts := make(map[string]int)

	for _, line := range outputLines {
		messageCounts[line]++
	}

	fmt.Printf(
		"Total empty messages: %d from total of %d.\n",
		messageCounts[""],
		len(messageCounts),
	)

	// Verify each expected message appears exactly once
	duplicates := []string{}
	missing := []string{}

	expectedMessages.Range(
		func(key, _ any) bool {
			raw, couldCast := key.(string)
			require.True(t, couldCast)

			// Normalize to how lines are stored in messageCounts
			msg := strings.TrimSuffix(raw, "\n")

			count := messageCounts[msg]

			if count == 0 {
				missing = append(missing, msg)
			} else if count > 1 {
				duplicates = append(duplicates, msg)
			}

			return true
		},
	)

	if len(missing) > 0 {
		helpers.DumpSyncMap(t, &expectedMessages)
	}

	require.Empty(t,
		missing,

		"Missing messages: %d with %d success messages", // fails with missing messages
		len(missing),
		successCount.Load(),
	)

	require.Empty(t,
		duplicates,

		"Duplicate messages detected: %d duplicates",
		len(duplicates))

	// Verify output line count matches success count
	require.Equal(t,
		int(successCount.Load()),
		len(outputLines),

		"Output lines (%d) should equal successful writes (%d)",
		len(outputLines),
		successCount.Load(),
	)

	// Additional corruption check: ensure no partial or overlapping writes
	// by verifying all bytes are printable ASCII (no garbage from overlapping)
	for _, line := range outputLines {
		for _, b := range []byte(line) {
			// All characters should be printable ASCII or newline
			require.True(t,
				(b >= 32 && b <= 126) || b == '\n' || b == '\t',

				"Non-printable byte %d found in output: %q",
				b,
				line,
			)
		}
	}
}

// BenchmarkReserveContention-16    	20811876	        59.76 ns/op	      24 B/op	       1 allocs/op
// BenchmarkReserveContention measures performance under high rollback pressure
func BenchmarkReserveContention(b *testing.B) {
	arenaSize := uint32(1000)
	writer := helpers.NoopWriter{}

	ingestor, errCrIngestor := NewIngestor(arenaSize, &writer)
	require.NoError(b, errCrIngestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	go func() {
		ingestor.consumerLoop(ctx)
	}()

	b.ResetTimer()
	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				payload := helpers.SprintfInt(
					"msg-%d",
					int(rand.Int63()),
				)

				_ = ingestor.write(
					uint32(len(payload)),

					func(dst []byte) {
						copy(dst, payload)
					},
				)
			}
		},
	)

	cancel()
	<-chIngestionEnd
}
