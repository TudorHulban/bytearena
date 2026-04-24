package bytearena

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

func Test_01_1_Ingestor_SingleWrite(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(
		Size100K(),
		&writer,
		WithUnblockMilliseconds(100),
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

func Test_01_2_Ingestor_SingleWrite_Parallel(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(1024, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := "hi!"

	helpers.TestParallel(t,
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

		errWriteShuttingDown,
	)

	cancel()
	<-chIngestionEnd

	require.True(t,
		ingestor.isStopped.Load(),
	)
}

func Test_01_3_Ingestor_ioWriter_Parallel(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(1024, &writer)
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := "hi!"

	helpers.TestParallel(t,
		16,
		100,

		func() error {
			_, errWrite := ingestor.Write([]byte(payload))

			return errWrite
		},

		errWriteShuttingDown,
	)

	cancel()
	<-chIngestionEnd

	require.True(t,
		ingestor.isStopped.Load(),
	)
}

func Test_01_4_CustomFlusherInvoked(t *testing.T) {
	var writer bytes.Buffer

	// Use functional option if available, or ensure same-package test
	ingestor, errCrIngestor := NewIngestor(
		_Size1K,
		&writer,
		WithTickThresholdMilliseconds(1),
	)
	require.NoError(t, errCrIngestor)

	// Track flush invocation
	flusherCalled := atomic.Bool{}

	originalFlusher := ingestor.flusher

	ingestor.flusher = func(a *arena) {
		flusherCalled.Store(true)

		originalFlusher(a) // Still do the real flush
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := make([]byte, _Size1K/len(ingestor.subRegions))
	copy(payload, "force seal")

	require.NoError(t,
		ingestor.write(
			uint32(len(payload)),
			func(dst []byte) {
				copy(dst, payload)
			},
		),
	)

	// Complete the write BEFORE signaling flush
	ingestor.signalFlush()
	time.Sleep(10 * time.Millisecond) // Let tick() run

	// Cancel and wait for shutdown
	cancel()

	select {
	case <-chIngestionEnd:
	case <-time.After(400 * time.Millisecond):
		t.Fatal("Ingestion did not exit")
	}

	require.True(t,
		flusherCalled.Load(),
		"custom flusher should have been invoked",
	)
}
