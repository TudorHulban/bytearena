package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

const (
	_PauseNanoseconds = 17
)

// All benchmarks were done on Rocky 10.

// go test -run '^$' -bench '^BenchmarkArena_ConstantPayload$' -benchmem

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkArena_ConstantPayload-16    	89981698	        13.60 ns/op	       0 B/op	       0 allocs/op
func BenchmarkArena_ConstantPayload(b *testing.B) {
	var writer helpers.CountWriterNoBuffer

	ingestor, _ := NewIngestor(
		Size500K(),
		&writer,
	)

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
		helpers.ParamsDetectStabilization[uint64]{
			InitialValue: writer.TotalBytesWritten.Load(),

			GetCurrentValue: func() uint64 {
				return writer.TotalBytesWritten.Load()
			},

			PauseFn:         func() { helpers.Pause(1) },
			PauseFnDuration: _PauseNanoseconds * time.Nanosecond,

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

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkArena_FormattedPayload-16    	46554699	        26.14 ns/op	       0 B/op	       0 allocs/op
func BenchmarkArena_FormattedPayload(b *testing.B) {
	writer := helpers.CountWriterNoBuffer{}

	ingestor, _ := NewIngestor(
		Size500K(),
		&writer,
	)

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
		helpers.ParamsDetectStabilization[uint64]{
			InitialValue: writer.TotalBytesWritten.Load(),

			GetCurrentValue: func() uint64 {
				return writer.TotalBytesWritten.Load()
			},

			PauseFn:         func() { helpers.Pause(1) },
			PauseFnDuration: _PauseNanoseconds * time.Nanosecond,

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

// go test -run '^$' -bench '^BenchmarkIngestor_ioWriter_End2End$' -benchmem

// BenchmarkIngestor_ioWriter_End2End-16    	59860435	        21.82 ns/op	        11.73 Gb/s	      71 B/op	       0 allocs/op
func BenchmarkIngestor_ioWriter_End2End(b *testing.B) {
	writer := helpers.CountWriterWithBuffer{}

	ingestor, _ := NewIngestor(
		Size1M(),
		&writer,
	)

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
			PauseFnDuration: _PauseNanoseconds * time.Nanosecond, // or the measured Pause(30) duration

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

// go test -run '^$' -bench '^BenchmarkIngestor_ioWriter_Noop$' -benchmem

// BenchmarkIngestor_ioWriter_Noop-16    	88305834	        13.55 ns/op	        18.89 Gb/s	       0 B/op	       0 allocs/op
func BenchmarkIngestor_ioWriter_Noop(b *testing.B) {
	ingestor, _ := NewIngestor(
		Size1M(),
		&helpers.NoopWriter{},
	)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") // 32 bytes

	b.ReportAllocs()

	start := time.Now()

	for b.Loop() {
		_, _ = ingestor.Write(payload)
	}

	stableTS, stabilisationOccured := helpers.DetectStabilization(
		helpers.ParamsDetectStabilization[uint64]{
			InitialValue: uint64(0),

			GetCurrentValue: func() uint64 {
				e1, e2 := ingestor.GetArenaEpochs()

				return e1 + e2
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

// go test -run '^$' -bench '^BenchmarkIngestor_MultipleSizes$' -benchmem

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_MultipleSizes/size_msg16_arena500K-16         	87050940	        12.96 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena500K-16         	83347240	        13.95 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena500K-16        	65489949	        18.42 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena500K-16       	34579744	        31.49 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg16_arena1M-16           	90196822	        12.94 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena1M-16           	81967728	        14.21 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena1M-16          	65769919	        18.56 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena1M-16         	39526436	        30.01 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg16_arena2M-16           	90564499	        12.60 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena2M-16           	85387354	        13.53 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena2M-16          	58958144	        20.44 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena2M-16         	42288590	        28.39 ns/op	       0 B/op	       0 allocs/op
func BenchmarkIngestor_MultipleSizes(b *testing.B) {
	sizesMessage := []int{16, 64, 256, 1024}
	sizesArena := []Size{Size500K, Size1M, Size2M}

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
