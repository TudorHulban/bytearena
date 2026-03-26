package bytearena_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena"
)

func TestHowToUse_As_ioWriter(t *testing.T) {
	writer := bytes.Buffer{}

	ingestor, errCrIngestor := bytearena.NewIngestor(
		bytearena.Size100K,
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

	ingestor.ReportTelemetry()

	cancel()
	<-chIngestionEnd

	require.Contains(t, writer.String(), payload)

	fmt.Println(writer.String())
}

func TestHowToUse_Directly(t *testing.T) {
	writer := bytes.Buffer{}

	ingestor, errCrIngestor := bytearena.NewIngestor(
		bytearena.Size100K,
		&writer,
	)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := "xxx"

	var arr [256]byte

	buf := arr[:0]

	buf = append(buf, []byte(payload)...)

	// if errors, arena full or no active arena — fall back or drop.
	region, errWrite := ingestor.TryWrite(uint32(len(buf)))
	require.NoError(t, errWrite)
	require.NotZero(t, region)

	defer ingestor.EndWrite(region)

	copy(region.Buf(), buf)

	cancel()
	<-chIngestionEnd

	require.Contains(t, writer.String(), payload)
}
