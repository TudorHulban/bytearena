package bytearena

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test Case: Ingestion should be started

// Test: Create an ingestor but do not start ingestion.
// Verifies: Ingestion is not automatically started,
// it should started for correct operation.

func Test_02_Error_NoIngestionStart(t *testing.T) {
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
