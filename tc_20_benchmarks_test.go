package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

const (
	_Pause1Nanoseconds = 17
)

// All benchmarks were done on Rocky 10.

// go test -run '^$' -bench '^BenchmarkArena_ConstantPayload$' -benchmem
// go test -run '^$' -bench '^BenchmarkArena_ConstantPayload$' -benchmem -race

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkArena_ConstantPayload-16    	100000000	        11.96 ns/op	       0 B/op	       0 allocs/op
func BenchmarkArena_ConstantPayload(b *testing.B) {
	writer := helpers.CountWriterNoBuffer{}

	ingestor, err := NewIngestor(Size500K(), &writer)
	require.NoError(b, err)
	require.NotNil(b, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := []byte(`{"level":"info","msg":"user login","user_id":123}`)

	b.ReportAllocs()

	start := time.Now()

	for b.Loop() {
		ingestor.write(
			uint32(len(payload)),

			func(destination []byte) {
				copy(destination, payload)
			},
		)
	}

	stableTS, stabilisationOccured := helpers.DetectStabilization(
		helpers.ParamsDetectStabilization[int64]{
			InitialValue: writer.TotalBytesWritten.Load(),

			GetCurrentValue: func() int64 {
				return writer.TotalBytesWritten.Load()
			},

			PauseFn:         func() { helpers.Pause(1) },
			PauseFnDuration: _Pause1Nanoseconds * time.Nanosecond,

			NumberStableSamples:  2,
			MaximumNumberSamples: 100,
		},
	)

	var elapsed time.Duration

	if stabilisationOccured {
		elapsed = stableTS.Sub(start)
	} else {
		// fallback: ingestion never stabilized within limits
		elapsed = time.Since(start)
	}

	b.ReportMetric(
		float64(elapsed.Nanoseconds())/float64(b.N),
		"ns/op",
	)

	cancel()
	<-chIngestionEnd
}

// go test -run '^$' -bench '^BenchmarkArena_FormattedPayload$' -benchmem
// go test -run '^$' -bench '^BenchmarkArena_FormattedPayload$' -benchmem -race

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkArena_FormattedPayload-12    	43534532	        28.83 ns/op	       0 B/op	       0 allocs/op
func BenchmarkArena_FormattedPayload(b *testing.B) {
	writer := helpers.CountWriterWithBuffer{}

	ingestor, err := NewIngestor(1024, &writer)
	require.NoError(b, err)
	require.NotNil(b, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	buf := make([]byte, 0, 64)

	var totalBytes int

	b.ReportAllocs()

	start := time.Now()

	for ix := 0; b.Loop(); ix++ {
		buf = buf[:0]
		buf = append(buf, `{"level":"info","msg":"user login","user_id":`...)
		buf = strconv.AppendInt(buf, int64(ix), 10)
		buf = append(buf, '}')

		totalBytes = totalBytes + len(buf)

		ingestor.write(
			uint32(len(buf)),

			func(destination []byte) {
				copy(destination, buf)
			},
		)
	}

	stableTS, stabilisationOccured := helpers.DetectStabilization(
		helpers.ParamsDetectStabilization[int64]{
			InitialValue: writer.TotalBytesWritten.Load(),

			GetCurrentValue: func() int64 {
				return writer.TotalBytesWritten.Load()
			},

			PauseFn:         func() { helpers.Pause(1) },
			PauseFnDuration: _Pause1Nanoseconds * time.Nanosecond,

			NumberStableSamples:  2,
			MaximumNumberSamples: 100,
		},
	)

	var elapsed time.Duration
	if stabilisationOccured {
		elapsed = stableTS.Sub(start)
	} else {
		elapsed = time.Since(start)
	}

	b.ReportMetric(
		float64(elapsed.Nanoseconds())/float64(b.N),
		"ns/op",
	)

	cancel()
	<-chIngestionEnd
}

// go test -run '^$' -bench '^BenchmarkIngestor_Write_End2End$' -benchmem
// go test -run '^$' -bench '^BenchmarkIngestor_Write_End2End$' -benchmem -race
// BenchmarkIngestor_Write_End2End-16      63739382                19.99 ns/op             12.81 Gb/s            67 B/op          0 allocs/op
func BenchmarkIngestor_Write_End2End(b *testing.B) {
	writer := helpers.CountWriterWithBuffer{}

	ingestor, err := NewIngestor(Size1M(), &writer)
	require.NoError(b, err)
	require.NotNil(b, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") // 32 bytes

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()

	for b.Loop() {
		_, _ = ingestor.Write(payload)
	}

	// --- QUIESCENCE DETECTION ---
	// Stop when TotalBytesWritten stops increasing for ~500 ns.
	last := writer.TotalBytesWritten.Load()

	for {
		helpers.Pause(30) // ~500 ns on ryzen 5800h

		current := writer.TotalBytesWritten.Load()
		if current == last {
			break // no progress → ingestion quiescent
		}

		last = current
	}

	elapsed := time.Since(start)

	b.StopTimer()

	// Override the default ns/op with true end-to-end ingestion time.
	b.ReportMetric(
		float64(elapsed.Nanoseconds())/float64(b.N),
		"ns/op",
	)

	// Gb/s throughput
	bytesWritten := float64(writer.TotalBytesWritten.Load())
	seconds := float64(elapsed.Nanoseconds()) / 1e9
	gbps := (bytesWritten * 8) / (seconds * 1e9)

	b.ReportMetric(gbps, "Gb/s")

	cancel()
	<-chIngestionEnd
}

// BenchmarkIngestor_Write_Noop-16    	95407026	        12.26 ns/op	        20.89 Gb/s	       0 B/op	       0 allocs/op
func BenchmarkIngestor_Write_Noop(b *testing.B) {
	writer := helpers.NoopWriter{}

	ingestor, err := NewIngestor(Size1M(), &writer)
	require.NoError(b, err)
	require.NotNil(b, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") // 32 bytes

	b.ReportAllocs()
	b.ResetTimer()

	start := time.Now()

	for b.Loop() {
		_, _ = ingestor.Write(payload)
	}

	// --- QUIESCENCE DETECTION ---
	// Ingestion is finished when the active arena stops switching.
	last := ingestor.active.Load()

	for {
		helpers.Pause(30) // ~500 ns on Ryzen 5800H

		current := ingestor.active.Load()
		if current == last {
			break // no arena rotation → ingestion quiescent
		}

		last = current
	}

	elapsed := time.Since(start)

	b.StopTimer()

	// ns/op (end-to-end ingestion)
	b.ReportMetric(
		float64(elapsed.Nanoseconds())/float64(b.N),
		"ns/op",
	)

	// Gb/s throughput (payload size × events)
	totalBytes := float64(b.N * len(payload))
	seconds := float64(elapsed.Nanoseconds()) / 1e9
	gbps := (totalBytes * 8) / (seconds * 1e9)

	b.ReportMetric(gbps, "Gb/s")

	cancel()
	<-chIngestionEnd
}

// go test -run '^$' -bench '^BenchmarkIngestor_WriteParallel$' -benchmem
// go test -run '^$' -bench '^BenchmarkIngestor_WriteParallel$' -benchmem -race
// BenchmarkIngestor_WriteParallel-12    	21925039	        54.43 ns/op	       0 B/op	       0 allocs/op
func BenchmarkIngestor_WriteParallel(b *testing.B) {
	writer := helpers.CountWriterWithBuffer{}

	ingestor, errCrIngestor := NewIngestor(Size100K(), &writer)
	require.NoError(b, errCrIngestor)
	require.NotNil(b, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

	b.ReportAllocs()
	b.SetParallelism(16) // tune accordingly
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
		writer.TotalBytesWritten.Load(),
		written.Load()*int64(len(payload)),
	)
}

// cpu: AMD Ryzen 5 5600U with Radeon Graphics
// BenchmarkIngestor_MultipleSizes/size_msg16_arena102400-12         	86013297	        13.91 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena102400-12         	77743675	        15.38 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena102400-12        	73199229	        16.32 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena102400-12       	72738548	        16.50 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg16_arena512000-12         	90128590	        12.82 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena512000-12         	84446210	        14.00 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena512000-12        	69715321	        16.45 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena512000-12       	65314560	        17.46 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg16_arena1048576-12        	95055610	        12.61 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena1048576-12        	88900497	        13.53 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena1048576-12       	74119821	        15.92 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena1048576-12      	65107002	        18.12 ns/op	       0 B/op	       0 allocs/op
func BenchmarkIngestor_MultipleSizes(b *testing.B) {
	sizesMessage := []int{16, 64, 256, 1024}
	sizesArena := []uint32{Size100K(), Size500K(), Size1M()}

	for _, sizeArena := range sizesArena {
		for _, sizeMessage := range sizesMessage {
			b.Run(
				fmt.Sprintf(
					"size_msg%d_arena%d",
					sizeMessage,
					sizeArena,
				),

				func(b *testing.B) {
					ingestor, errCrIngestor := NewIngestor(
						sizeArena,
						&helpers.NoopWriter{},
					)
					require.NoError(b, errCrIngestor)
					require.NotNil(b, ingestor)

					ctx, cancel := context.WithCancel(context.Background())
					chIngestionEnd := ingestor.StartIngestion(ctx)

					payload := bytes.Repeat([]byte("x"), sizeMessage)

					b.ReportAllocs()
					b.ResetTimer()

					for b.Loop() {
						_, _ = ingestor.Write(payload)
					}

					cancel()
					<-chIngestionEnd
				},
			)
		}
	}
}
