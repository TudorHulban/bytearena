package bytearena_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena"
)

func TestHowToUse(t *testing.T) {
	writer := bytes.Buffer{}

	ingestor, errCrIngestor := bytearena.NewIngestor(
		bytearena.Size100K(),
		&writer,
	)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := "xxx"

	bytesWritten, errWrite := ingestor.Write([]byte(payload))
	require.NoError(t, errWrite)
	require.Equal(t, len(payload), bytesWritten)

	reporter := ingestor

	ingestor.ReportTelemetry(reporter)

	cancel()
	<-chIngestionEnd

	require.Contains(t, writer.String(), payload)

	fmt.Println(writer.String())
}
