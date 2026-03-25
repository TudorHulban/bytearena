package bytearena

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test Case 01: Ingestion should be started

// Test: Create an ingestor but do not start ingestion.
// Verifies: Ingestion is not automatically started,
// it should started for correct operation.

func TestError_NoIngestionStart(t *testing.T) {
	var sink bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(Size100K, &sink)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	_, cancel := context.WithCancel(context.Background())

	payload := "xxx"

	bytesWritten, errWrite := ingestor.Write([]byte(payload))
	require.NoError(t, errWrite)
	require.Equal(t, len(payload), bytesWritten)

	cancel()

	require.NotContains(t, sink.String(), payload)
}

func TestIngestor_SingleWrite(t *testing.T) {
	var out bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(1024, &out)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := "hi!"

	require.NoError(t,
		ingestor.write(
			uint32(len(payload)),

			func(destination []byte) {
				copy(destination, []byte(payload))
			},
		),
	)

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

	require.Equal(t,
		payload,
		out.String(),
	)
}
