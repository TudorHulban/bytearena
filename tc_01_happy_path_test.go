package bytearena

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

func Test_01_Ingestor_SingleWrite(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(
		Size100K(),
		&writer,
		WithUnblockFlushMiliseconds(100),
	)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)

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

	time.Sleep(10 * time.Millisecond)

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

func Test_01_Ingestor_SingleWrite_Parallel(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(1024, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)

	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := "hi!"

	helpers.RunParallel(t,
		16,
		100,

		func() error {
			return ingestor.write(
				uint32(len(payload)),

				func(destination []byte) {
					copy(destination, payload)
				},
			)
		},

		ErrWriteShuttingDown,
	)

	cancel()

	// Wait for consumer shutdown flush.
	<-chIngestionEnd

	require.True(t,
		ingestor.isStopped.Load(),
	)
}

func Test_01_Ingestor_ioWriter_Parallel(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(1024, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)

	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := "hi!"

	helpers.RunParallel(t,
		16,
		100,

		func() error {
			_, errWrite := ingestor.Write([]byte(payload))

			return errWrite
		},

		ErrWriteShuttingDown,
	)

	cancel()
	<-chIngestionEnd

	require.True(t,
		ingestor.isStopped.Load(),
	)
}
