package bytearena_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena"
	"github.com/tudorhulban/bytearena/helpers"
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

func TestTryWrite(t *testing.T) {
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

	var arr [256]byte

	buf := append(arr[:0], []byte(payload)...)

	region, errWrite := ingestor.TryWrite(uint32(len(buf)))
	require.NoError(t, errWrite)
	require.NotZero(t, region)

	copy(region.Buf(), buf)

	// must happen before cancel — flushOnShutdown waits for writers
	ingestor.EndWrite(region)

	cancel()
	<-chIngestionEnd

	require.Contains(t, writer.String(), payload)
}

// BenchmarkIngestor_External-16    	44423943	        28.63 ns/op	       0 B/op	       0 allocs/op
func BenchmarkIngestor_External(b *testing.B) {
	ingestor, _ := bytearena.NewIngestor(
		bytearena.Size4M(),
		&helpers.NoopWriter{},
	)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	time.Sleep(10 * time.Millisecond) //warmup

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") // 32 bytes

	b.ReportAllocs()
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				_, _ = ingestor.Write(payload)
			}
		},
	)

	cancel()
	<-chIngestionEnd
}

// go test -run '^$' -bench '^BenchmarkIngestor_Region_Parallel$' -benchmem -race

// BenchmarkIngestor_Region_Parallel-16    	24276568	        56.08 ns/op	       0 B/op	       0 allocs/op
func BenchmarkIngestor_Region_Parallel(b *testing.B) {
	ingestor, _ := bytearena.NewIngestor(
		bytearena.Size4M(),
		&helpers.NoopWriter{},
	)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	time.Sleep(10 * time.Millisecond) // warmup

	b.ReportAllocs()
	b.SetParallelism(16)
	b.ResetTimer()

	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				region, errWrite := ingestor.TryWrite(32)
				if errWrite != nil {
					continue
				}

				copy(
					region.Buf(),
					[]byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
				)

				ingestor.EndWrite(region)
			}
		},
	)

	cancel()
	<-chIngestionEnd
}
