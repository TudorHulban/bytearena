package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// All benchmarks were done on Rocky 10.
// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkArena_ConstantPayload-16    	87104852	        13.50 ns/op	       0 B/op	       0 allocs/op
func BenchmarkArena_ConstantPayload(b *testing.B) {
	writer := helpers.CountWriter{}

	ingestor, errCrIngestor := NewIngestor(1024*1024, &writer)
	require.NoError(b, errCrIngestor)
	require.NotNil(b, ingestor)

	payload := []byte(`{"level":"info","msg":"user login","user_id":123}`)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		ingestor.write(
			uint32(len(payload)),

			func(destination []byte) {
				copy(destination, payload)
			},
		)
	}

	_ = writer.TotalBytesWritten.Load()
}

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkArena_FormattedPayload-16    	42669784	        29.53 ns/op	       0 B/op	       0 allocs/op
func BenchmarkArena_FormattedPayload(b *testing.B) {
	writer := helpers.CountWriter{}

	ingestor, errCrIngestor := NewIngestor(1024, &writer)
	require.NoError(b, errCrIngestor)
	require.NotNil(b, ingestor)

	b.ReportAllocs()
	b.ResetTimer()

	// [ADD] Reusable buffer for zero-alloc JSON construction
	buf := make([]byte, 0, 64)

	for ix := 0; b.Loop(); ix++ {
		// [EDIT] Zero-alloc JSON construction
		buf = buf[:0]
		buf = append(buf, `{"level":"info","msg":"user login","user_id":`...)
		buf = strconv.AppendInt(buf, int64(ix), 10)
		buf = append(buf, '}')

		// [EDIT] Write bytes directly, no string conversion
		ingestor.write(
			uint32(len(buf)),

			func(destination []byte) {
				copy(destination, buf)
			},
		)
	}

	_ = writer.TotalBytesWritten.Load() // keep sink live
}

// go test -run '^$' -bench '^BenchmarkIngestor_Write$' -benchmem
// go test -run '^$' -bench '^BenchmarkIngestor_Write$' -benchmem -race
// BenchmarkIngestor_Write-16    	99849817	        12.45 ns/op	       0 B/op	       0 allocs/op
func BenchmarkIngestor_Write(b *testing.B) {
	writer := helpers.CountWriter{}

	ingestor, errCrIngestor := NewIngestor(Size1M(), &writer)
	require.NoError(b, errCrIngestor)
	require.NotNil(b, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	payload := []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

	b.ReportAllocs()
	b.ResetTimer()

	var written atomic.Int64

	for b.Loop() {
		if _, errWrite := ingestor.Write(payload); errWrite == nil {
			written.Add(1)
		}
	}

	cancel()
	<-chIngestionEnd

	b.Log(written.Load())

	require.EqualValues(b,
		writer.TotalBytesWritten.Load(),
		written.Load()*int64(len(payload)),
	)
}

// go test -run '^$' -bench '^BenchmarkIngestor_WriteParallel$' -benchmem
// go test -run '^$' -bench '^BenchmarkIngestor_WriteParallel$' -benchmem -race
// BenchmarkIngestor_WriteParallel-16    	21970140	        56.18 ns/op	       0 B/op	       0 allocs/op
func BenchmarkIngestor_WriteParallel(b *testing.B) {
	writer := helpers.CountWriter{}

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

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_MultipleSizes/size_msg16_arena102400-16         	90126430	        13.89 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena102400-16         	76402267	        15.97 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena102400-16        	62926371	        17.55 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena102400-16       	67831453	        17.47 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg16_arena512000-16         	97691055	        12.18 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena512000-16         	86874651	        14.09 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena512000-16        	71361512	        16.78 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena512000-16       	63043191	        18.59 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg16_arena1048576-16        	99691752	        11.89 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg64_arena1048576-16        	88871469	        13.22 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg256_arena1048576-16       	73939690	        16.17 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/size_msg1024_arena1048576-16      	62773869	        19.28 ns/op	       0 B/op	       0 allocs/op
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
