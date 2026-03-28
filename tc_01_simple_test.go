package bytearena

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// Test Case 01: Ingestion should be started

// Test: Create an ingestor but do not start ingestion.
// Verifies: Ingestion is not automatically started,
// it should started for correct operation.

func Test_01_a_Error_NoIngestionStart(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(Size100K(), &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	_, cancel := context.WithCancel(context.Background())

	payload := "xxx"

	bytesWritten, errWrite := ingestor.Write([]byte(payload))
	require.NoError(t, errWrite)
	require.Equal(t, len(payload), bytesWritten)

	cancel()

	require.NotContains(t, writer.String(), payload)
}

func Test_01_b_Ingestor_SingleWrite(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(1024, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)

	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := "hi!"

	require.NoError(t,
		ingestor.write(
			uint32(len(payload)),

			func(destination []byte) {
				copy(destination, payload)
			},
		),
	)

	cancel()

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

	require.Equal(t,
		payload,
		writer.String(),
	)
}

func Test_01_c_Ingestor_OversizeWrite(t *testing.T) {
	var writer helpers.NoopWriter

	ingestor, errCrIngestor := NewIngestor(
		1,
		&writer,
		WithTelemetry(),
		WithTelemetryWriter(os.Stdout),
	)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)

	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := "hi!"

	require.ErrorIs(t,
		ingestor.write(
			uint32(len(payload)),

			func(destination []byte) {
				copy(destination, payload)
			},
		),

		ErrWriteMessageTooLarge,
	)

	cancel()

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

	require.Zero(t,
		ingestor.Metrics.NumberRollbacks.Load(),
	)

	ingestor.ReportTelemetry(ingestor)

	ingestor.Metrics.Reset()

	require.Zero(t,
		ingestor.Metrics.NumberRollbacks.Load(),
	)
}
