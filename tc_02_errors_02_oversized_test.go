package bytearena

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

func Test_02_02_Ingestor_OversizeWrite(t *testing.T) {
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
