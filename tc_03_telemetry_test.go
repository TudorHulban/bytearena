package bytearena

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// Test case: Telemetry Rollback Attribute

// Test: Verifies telemetry rollback is incremented at least once.
// Note: the consumer is async and there is no
// synchronization point between producer retry and consumer rotation.

func Test_03_Ingestor_CheckRollback(t *testing.T) {
	var writer helpers.CountWriterWithBuffer

	ingestor, errCrIngestor := NewIngestor(
		200,
		&writer,
		WithTelemetry(),
		WithTelemetryWriter(os.Stdout),
	)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	cursorsInit := ingestor.arenaFirst.getCursorValues()

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

	errWrite3 := ingestor.write(
		uint32(len(payload3)),

		func(destination []byte) {
			copy(destination, payload3)
		},
	)
	if errWrite3 != nil {
		require.ErrorIs(t, errWrite3, ErrWriteShuttingDown)
	}

	cancel()

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

	fmt.Println(
		cursorsInit,
		ingestor.arenaFirst.getCursorValues(),
	)

	require.Contains(t,
		writer.Buf.String(),
		string(payload1),
	)

	assert.EqualValues(t,
		3,
		writer.NumberWrites.Load(),
	)
	assert.EqualValues(t,
		60,
		writer.TotalBytesWritten.Load(),
	)

	require.GreaterOrEqual(t,
		ingestor.arenaFirst.epoch.Load(),
		uint64(0),
	)

	require.GreaterOrEqual(t,
		ingestor.Metrics.NumberRollbacks.Load(),
		uint64(0),

		"number rollbacks",
	)

	ingestor.ReportTelemetry(ingestor)
}
