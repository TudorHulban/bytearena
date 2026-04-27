package bytearena_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
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

// go test -run '^$' -bench '^BenchmarkLogger_Parallel$' -benchmem -race

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkLogger_Parallel/gomaxprocs=1-16         	37631481	        31.82 ns/op	       0 B/op	       0 allocs/op
// BenchmarkLogger_Parallel/gomaxprocs=2-16         	15584614	        81.69 ns/op	       0 B/op	       0 allocs/op
// BenchmarkLogger_Parallel/gomaxprocs=8-16         	20943333	        66.29 ns/op	       0 B/op	       0 allocs/op
// BenchmarkLogger_Parallel/gomaxprocs=16-16        	22825214	        54.55 ns/op	       0 B/op	       0 allocs/op
func BenchmarkLogger_Parallel(b *testing.B) {
	gomaxValues := []int{1, 2, 8, 16}

	ingestor, _ := bytearena.NewIngestor(
		bytearena.Size4M(),
		&helpers.NoopWriter{},
	)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	time.Sleep(10 * time.Millisecond) // warmup

	for _, v := range gomaxValues {
		b.Run(
			fmt.Sprintf("gomaxprocs=%d", v),
			func(b *testing.B) {
				prev := runtime.GOMAXPROCS(v)
				defer runtime.GOMAXPROCS(prev)

				b.SetParallelism(16)
				b.ReportAllocs()
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
			},
		)
	}

	cancel()
	<-chIngestionEnd
}
