package bytearena

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_01_Ingestor_SingleWrite(t *testing.T) {
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

	require.True(t,
		ingestor.isStopped.Load(),
	)

	require.Equal(t,
		payload,
		writer.String(),
	)
}
