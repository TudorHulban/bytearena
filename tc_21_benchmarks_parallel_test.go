package bytearena

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// go test -run '^$' -bench '^BenchmarkIngestor_ioWriter_Parallel$' -benchmem
// go test -run '^$' -bench '^BenchmarkIngestor_ioWriter_Parallel$' -benchmem -race

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_Parallel/GOMAXPROCS=1-16         	93682153	        12.69 ns/op	        20.16 Gb/s	       0 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel/GOMAXPROCS=2-16         	23365327	        51.60 ns/op	         4.961 Gb/s	       0 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel/GOMAXPROCS=3-16         	24524922	        49.45 ns/op	         5.174 Gb/s	       0 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel/GOMAXPROCS=4-16         	27625579	        46.27 ns/op	         5.526 Gb/s	       0 B/op	       0 allocs/op
func BenchmarkIngestor_Parallel(b *testing.B) {
	gomaxprocsValues := []int{1, 2, 3, 4}
	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") // 32 bytes

	for _, g := range gomaxprocsValues {
		b.Run(
			fmt.Sprintf("GOMAXPROCS=%d", g),
			func(b *testing.B) {
				prev := runtime.GOMAXPROCS(g)
				defer runtime.GOMAXPROCS(prev)

				writer := helpers.CountWriterNoBuffer{}

				ingestor, err := NewIngestor(
					Size1M(),
					&writer,
				)
				require.NoError(b, err)

				ctx, cancel := context.WithCancel(context.Background())
				chIngestionEnd := ingestor.StartIngestion(ctx)

				time.Sleep(10 * time.Millisecond)

				runtime.GC()

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

				b.StopTimer()

				cancel()
				<-chIngestionEnd

				bytesWritten := float64(writer.TotalBytesWritten.Load())
				seconds := float64(b.Elapsed().Nanoseconds()) / 1e9
				gbps := (bytesWritten * 8) / (seconds * 1e9)

				b.ReportMetric(gbps, "Gb/s")
			},
		)
	}
}

// go test -run '^$' -bench '^BenchmarkIngestor_Parallel_BytesWritten$' -benchmem
// go test -run '^$' -bench '^BenchmarkIngestor_Parallel_BytesWritten$' -benchmem -race

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_Parallel_BytesWritten/GOMAXPROCS=1-16         	41361720	        28.64 ns/op	     135 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel_BytesWritten/GOMAXPROCS=2-16         	26511316	        61.31 ns/op	     112 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel_BytesWritten/GOMAXPROCS=3-16         	19635298	        70.94 ns/op	     141 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel_BytesWritten/GOMAXPROCS=4-16         	22517086	        60.96 ns/op	     127 B/op	       0 allocs/op
func BenchmarkIngestor_Parallel_BytesWritten(b *testing.B) {
	gomaxprocsValues := []int{1, 2, 3, 4}
	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") // 32 bytes

	for _, g := range gomaxprocsValues {
		b.Run(
			fmt.Sprintf("GOMAXPROCS=%d", g),
			func(b *testing.B) {
				prev := runtime.GOMAXPROCS(g)
				defer runtime.GOMAXPROCS(prev)

				writer := helpers.CountWriterWithBuffer{}

				ingestor, err := NewIngestor(
					Size2M(),
					&writer,
					WithIsolatedBufferFlusher(),
				)
				require.NoError(b, err)

				ctx, cancel := context.WithCancel(context.Background())
				chIngestionEnd := ingestor.StartIngestion(ctx)

				time.Sleep(10 * time.Millisecond)

				runtime.GC()

				b.ReportAllocs()
				b.SetParallelism(16)
				b.ResetTimer()

				var written atomic.Int64

				b.RunParallel(
					func(pb *testing.PB) {
						for pb.Next() {
							_, errWrite := ingestor.Write(payload)
							if errWrite == nil {
								written.Add(1)
							}
						}
					},
				)

				b.StopTimer()

				cancel()
				<-chIngestionEnd

				require.EqualValues(
					b,
					written.Load()*int64(len(payload)),
					writer.TotalBytesWritten.Load(),
				)
			},
		)
	}
}
