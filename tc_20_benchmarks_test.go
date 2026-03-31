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

	ingestor, errCrIngestor := NewIngestor(Size500K(), &writer)
	require.NoError(b, errCrIngestor)
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
// BenchmarkArena_FormattedPayload-16      50439084                24.85 ns/op            0 B/op          0 allocs/op
func BenchmarkArena_FormattedPayload(b *testing.B) {
	writer := helpers.CountWriterNoBuffer{}

	ingestor, errCrIngestor := NewIngestor(Size500K(), &writer)
	require.NoError(b, errCrIngestor)
	require.NotNil(b, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	buf := make([]byte, 0, 64)

	b.ReportAllocs()

	start := time.Now()

	for ix := 0; b.Loop(); ix++ {
		buf = buf[:0]
		buf = append(buf, `{"level":"info","msg":"user login","user_id":`...)
		buf = strconv.AppendInt(buf, int64(ix), 10)
		buf = append(buf, '}')

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

	start := time.Now()

	for b.Loop() {
		_, _ = ingestor.Write(payload)
	}

	stableTS, ok := helpers.DetectStabilization(
		helpers.ParamsDetectStabilization[int64]{
			InitialValue: writer.TotalBytesWritten.Load(),

			GetCurrentValue: func() int64 {
				return writer.TotalBytesWritten.Load()
			},

			PauseFn:         func() { helpers.Pause(1) },
			PauseFnDuration: _Pause1Nanoseconds * time.Nanosecond, // or the measured Pause(30) duration

			NumberStableSamples:  2,   // require 2 identical samples
			MaximumNumberSamples: 100, // safety cap
		},
	)

	var elapsed time.Duration

	if ok {
		elapsed = stableTS.Sub(start)
	} else {
		elapsed = time.Since(start)
	}

	// Override the default ns/op with true end-to-end ingestion time.
	b.ReportMetric(
		float64(elapsed.Nanoseconds())/float64(b.N),
		"ns/op",
	)

	bytesWritten := float64(writer.TotalBytesWritten.Load())
	seconds := float64(elapsed.Nanoseconds()) / 1e9
	gbps := (bytesWritten * 8) / (seconds * 1e9)

	b.ReportMetric(gbps, "Gb/s") // Gb/s throughput

	cancel()
	<-chIngestionEnd
}

// go test -run '^$' -bench '^BenchmarkIngestor_Write_Parallel$' -benchmem

func BenchmarkIngestor_Write_Parallel(b *testing.B) {
	writer := helpers.CountWriterNoBuffer{}

	ingestor, err := NewIngestor(Size1M(), &writer)
	require.NoError(b, err)
	require.NotNil(b, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") // 32 bytes

	b.ReportAllocs()
	b.SetParallelism(2)

	start := time.Now()

	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				_, _ = ingestor.Write(payload)
			}
		},
	)

	stableTS, ok := helpers.DetectStabilization(
		helpers.ParamsDetectStabilization[int64]{
			InitialValue: writer.TotalBytesWritten.Load(),

			GetCurrentValue: func() int64 {
				return writer.TotalBytesWritten.Load()
			},

			PauseFn:         func() { helpers.Pause(1) },
			PauseFnDuration: _Pause1Nanoseconds * time.Nanosecond, // or the measured Pause(30) duration

			NumberStableSamples:  2,   // require 2 identical samples
			MaximumNumberSamples: 100, // safety cap
		},
	)

	var elapsed time.Duration

	if ok {
		elapsed = stableTS.Sub(start)
	} else {
		elapsed = time.Since(start)
	}

	// Override the default ns/op with true end-to-end ingestion time.
	b.ReportMetric(
		float64(elapsed.Nanoseconds())/float64(b.N),
		"ns/op",
	)

	bytesWritten := float64(writer.TotalBytesWritten.Load())
	seconds := float64(elapsed.Nanoseconds()) / 1e9
	gbps := (bytesWritten * 8) / (seconds * 1e9)

	b.ReportMetric(gbps, "Gb/s") // Gb/s throughput

	cancel()
	<-chIngestionEnd
}

// go test -run '^$' -bench '^BenchmarkIngestor_Write_Noop$' -benchmem

// BenchmarkIngestor_Write_Noop-16    	98553408	        12.00 ns/op	        21.34 Gb/s	       0 B/op	       0 allocs/op
func BenchmarkIngestor_Write_Noop(b *testing.B) {
	writer := helpers.NoopWriter{}

	ingestor, err := NewIngestor(Size1M(), &writer)
	require.NoError(b, err)
	require.NotNil(b, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") // 32 bytes

	b.ReportAllocs()

	start := time.Now()

	for b.Loop() {
		_, _ = ingestor.Write(payload)
	}

	stableTS, stabilisationOccured := helpers.DetectStabilization(
		helpers.ParamsDetectStabilization[*arena]{
			InitialValue: ingestor.active.Load(),

			GetCurrentValue: func() *arena {
				return ingestor.active.Load()
			},

			PauseFn:         func() { helpers.Pause(30) }, // ~500ns on your hardware
			PauseFnDuration: 500 * time.Nanosecond,        // or measured Pause(30)

			NumberStableSamples:  2,   // require 2 identical samples
			MaximumNumberSamples: 500, // safety cap
		},
	)

	var elapsed time.Duration

	if stabilisationOccured {
		elapsed = stableTS.Sub(start)
	} else {
		elapsed = time.Since(start)
	}

	// ns/op (end-to-end ingestion)
	b.ReportMetric(
		float64(elapsed.Nanoseconds())/float64(b.N),
		"ns/op",
	)

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
	b.SetParallelism(7)
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

// go test -run '^$' -bench '^BenchmarkIngestor_MultipleSizes$' -benchmem

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_MultipleSizes/size_msg16_arena100K-16                 100000000               11.91 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena100K-16                 87054337                13.06 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena100K-16                64573251                18.67 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena100K-16               26429690                45.41 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg16_arena500K-16                 99679041                11.69 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena500K-16                 88759490                12.51 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena500K-16                70224813                16.59 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena500K-16               39498555                29.10 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg16_arena1M-16                   98174935                11.58 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena1M-16                   92375154                12.43 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena1M-16                  70577258                15.45 ns/op            0 B/op          0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena1M-16                 43015221                26.37 ns/op            0 B/op          0 allocs/op
func BenchmarkIngestor_MultipleSizes(b *testing.B) {
	sizesMessage := []int{16, 64, 256, 1024}
	sizesArena := []Size{Size100K, Size500K, Size1M}

	for _, sizeArena := range sizesArena {
		for _, sizeMessage := range sizesMessage {
			b.Run(
				fmt.Sprintf(
					"size_msg%d_arena%s",
					sizeMessage,
					sizeArena.String(),
				),

				func(b *testing.B) {
					ingestor, errCrIngestor := NewIngestor(
						sizeArena(),
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
