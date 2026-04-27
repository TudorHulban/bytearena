package bytearena

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// go test -run '^$' -bench '^BenchmarkIngestor_ioWriter_Parallel$' -benchmem
// go test -run '^$' -bench '^BenchmarkIngestor_ioWriter_Parallel$' -benchmem -race

// BenchmarkIngestor_ioWriter_Parallel-16    	41500706	        29.62 ns/op	         8.631 Gb/s	       0 B/op	       0 allocs/op
func BenchmarkIngestor_Parallel(b *testing.B) {
	writer := helpers.CountWriterNoBuffer{}

	ingestor, _ := NewIngestor(
		Size1M(),
		&writer,
	)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	time.Sleep(10 * time.Millisecond) // warmup

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") // 32 bytes

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

	// Throughput
	bytesWritten := float64(writer.TotalBytesWritten.Load())
	seconds := float64(b.Elapsed().Nanoseconds()) / 1e9
	gbps := (bytesWritten * 8) / (seconds * 1e9)

	b.ReportMetric(gbps, "Gb/s")

	cancel()
	<-chIngestionEnd
}

// go test -run '^$' -bench '^BenchmarkIngestor_Parallel_BytesWritten$' -benchmem
// go test -run '^$' -bench '^BenchmarkIngestor_Parallel_BytesWritten$' -benchmem -race

// BenchmarkIngestor_Parallel_BytesWritten-16    	33658668	        46.24 ns/op	     159 B/op	       0 allocs/op
func BenchmarkIngestor_Parallel_BytesWritten(b *testing.B) {
	writer := helpers.CountWriterWithBuffer{}

	ingestor, _ := NewIngestor(
		Size2M(),
		&writer,
		WithIsolatedBufferFlusher(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	time.Sleep(10 * time.Millisecond) // warmup

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

	runtime.GC()

	b.ReportAllocs()
	b.SetParallelism(16)
	b.ResetTimer()

	var written atomic.Int64

	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				if _, errWrite := ingestor.Write(payload); errWrite == nil {
					written.Add(1)
				}
			}
		},
	)

	cancel()
	<-chIngestionEnd

	b.Log(written.Load())

	require.EqualValues(b,
		written.Load()*int64(len(payload)),
		writer.TotalBytesWritten.Load(),
	)
}
