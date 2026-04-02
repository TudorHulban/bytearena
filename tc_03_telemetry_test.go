package bytearena

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// Test case: Telemetry Rollback

// Test: Verifies telemetry rollback is incremented at least once.
// Note: the consumer is async and there is no
// synchronization point between producer retry and consumer rotation.

func Test_03_01_Ingestor_CheckRollback(t *testing.T) {
	var writer helpers.CountWriterWithBuffer

	ingestor, errCrIngestor := NewIngestor(
		50,
		&writer,
		WithTelemetry(),
		WithTelemetryWriter(os.Stdout),
	)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)

	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload1 := helpers.MakePayloadNumbered(20, 1, 'x')

	require.NoError(t,
		ingestor.write(
			uint32(len(payload1)),

			func(destination []byte) {
				copy(destination, payload1)
			},
		),
	)

	payload2 := helpers.MakePayloadNumbered(20, 2, 'y')

	require.NoError(t,
		ingestor.write(
			uint32(len(payload2)),

			func(destination []byte) {
				copy(destination, payload2)
			},
		),
	)

	payload3 := helpers.MakePayloadNumbered(20, 3, 'z')

	require.NoError(t,
		ingestor.write(
			uint32(len(payload3)),

			func(destination []byte) {
				copy(destination, payload3)
			},
		),
	)

	cancel()

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

	e1, _ := ingestor.GetArenaEpochs()

	require.EqualValues(t,
		1,
		e1,

		"one rotation should have occurred",
	)

	require.Contains(t,
		writer.Buf.String(),
		string(payload1),
	)

	// arena flush #1 → arenaFirst  → payload1 + payload2 (40 bytes) → Write call #1
	// arena flush #2 → arenaSecond → payload3 (20 bytes)            → Write call #2
	assert.EqualValues(t,
		2,
		writer.NumberWrites.Load(),
	)
	assert.EqualValues(t,
		60,
		writer.TotalBytesWritten.Load(),
	)

	require.GreaterOrEqual(t,
		ingestor.arenaFirst.epoch.Load(),
		uint64(1),
	)

	require.GreaterOrEqual(t,
		ingestor.Metrics.NumberRollbacks.Load(),
		uint64(1),

		"number rollbacks",
	)

	// 	TryWrite
	//  └─ beginWrite attempt #1
	//        cur=40 > limit=30  →  rollback #1, signalFlush, ErrWriteArenaFull
	//  └─ not ErrWriteMessageTooLarge, so retries immediately:
	//  └─ beginWrite attempt #2
	//        cur=40 > limit=30  →  rollback #2, signalFlush, ErrWriteArenaFull
	//  └─ returns ErrWriteArenaFull

	// TryWrite retries on any error except ErrWriteMessageTooLarge.
	// Both attempts land on the same still-full arenaFirst because the consumer goroutine has not been scheduled yet.
	// signalFlush only sends to a channel, the actual rotation is async.
	// The more rollbacks are not a bug; they are an accurate count of failed reservation attempts.

	ingestor.ReportTelemetry(ingestor)
}
