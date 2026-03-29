package bytearena

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

func TestZeroProgressWriter(t *testing.T) {
	var writer helpers.ZeroProgressWriter

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
}
