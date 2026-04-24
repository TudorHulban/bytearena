package bytearena

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_02_Ingestor_OversizeWrite(t *testing.T) {
	var writer bytes.Buffer

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

		errWriteMessageTooLarge,
	)

	cancel()

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

	require.Zero(t,
		ingestor.Metrics.NumberRollbacks.Load(),
	)

	require.EqualValues(t,
		1,
		ingestor.Registry.load(TErrWriteMessageTooLarge),
	)

	ingestor.ReportTelemetry(ingestor)

	require.Zero(t,
		ingestor.Metrics.NumberRollbacks.Load(),
	)
}
