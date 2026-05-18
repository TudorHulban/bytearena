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
// BenchmarkIngestor_Parallel/GOMAXPROCS=1-16         	90192199	        13.10 ns/op	        19.54 Gb/s	       0 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel/GOMAXPROCS=2-16         	29095045	        42.06 ns/op	         6.087 Gb/s	       0 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel/GOMAXPROCS=3-16         	29425225	        41.40 ns/op	         6.183 Gb/s	       0 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel/GOMAXPROCS=4-16         	32604859	        37.51 ns/op	         6.825 Gb/s	       0 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel/GOMAXPROCS=8-16         	35401309	        35.73 ns/op	         7.164 Gb/s	       0 B/op	       0 allocs/op
func BenchmarkIngestor_Parallel(b *testing.B) {
	gomaxprocsValues := []int{1, 2, 3, 4, 8}
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

					helpers.TernaryWithValueIn(
						[]int{1},
						g,
						nil,
						WithCounterCoreCPU(),
					),
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
// BenchmarkIngestor_Parallel_BytesWritten/GOMAXPROCS=1-16         	47491737	        26.76 ns/op	     122 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel_BytesWritten/GOMAXPROCS=2-16         	27467739	        52.55 ns/op	     110 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel_BytesWritten/GOMAXPROCS=3-16         	24730060	        57.59 ns/op	     118 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel_BytesWritten/GOMAXPROCS=4-16         	25296752	        54.81 ns/op	     116 B/op	       0 allocs/op
// BenchmarkIngestor_Parallel_BytesWritten/GOMAXPROCS=8-16         	31728002	        48.30 ns/op	      99 B/op	       0 allocs/op
func BenchmarkIngestor_Parallel_BytesWritten(b *testing.B) {
	gomaxprocsValues := []int{1, 2, 3, 4, 8}
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
					helpers.TernaryWithValueIn(
						[]int{1},
						g,
						nil,
						WithCounterCoreCPU(),
					),
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
