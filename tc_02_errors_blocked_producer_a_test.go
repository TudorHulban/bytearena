package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// Blocked producer would actually block the ingestion.

func Test_BlockedProducer(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(_Size1K, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)

	chIngestionEnd := ingestor.StartIngestion(ctx)

	// stuck producer

	// Step 1: reserve a region1 but DO NOT write yet
	region1, errWrite1 := ingestor.beginWrite(50)
	require.NoError(t, errWrite1)
	require.NotZero(t, region1)

	payload1 := helpers.MakePayloadWLineFeed(50, 1, 's')

	// normal message

	payload2 := helpers.MakePayloadWLineFeed(50, 2, 'x')

	bytesWritten2, errWrite2 := ingestor.Write(payload2)
	require.NoError(t, errWrite2)
	require.Equal(t, len(payload2), bytesWritten2)

	// normal message

	payload3 := helpers.MakePayloadWLineFeed(50, 3, 'y')

	bytesWritten3, errWrite3 := ingestor.Write(payload3)
	require.NoError(t, errWrite3)
	require.Equal(t, len(payload3), bytesWritten3)

	ingestor.EndWrite(region1)

	phase1E1, phase1E2 := ingestor.GetArenaEpochs()

	fmt.Println("phase 1:", phase1E1, phase1E2) // 0,0

	ingestor.write(
		uint32(len(payload1)),

		func(destination []byte) {
			copy(destination, payload1)
		},
	)

	phase2E1, phase2E2 := ingestor.GetArenaEpochs()

	fmt.Println("phase 2:", phase2E1, phase2E2) // 0,0

	require.NotNil(t,
		ingestor.active.Load(),
	)

	cancel() // unblocks stuck producer

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

	require.Nil(t,
		ingestor.active.Load(),
	)

	phase3E1, phase3E2 := ingestor.GetArenaEpochs()

	fmt.Println("phase 3:", phase3E1, phase3E2)

	fmt.Println(writer.String())
}
