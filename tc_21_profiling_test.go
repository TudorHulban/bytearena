package bytearena

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// What's Happening in Each write() Call
// 1. m.active.Load()                          // atomic pointer load
// 2. arena.Enter()                            // atomic.Add(&numberWriters, 1)
// 3. arena.cursor.Load() + CAS loop           // reservation with potential retries
// 4. fn(region.Buf())                         // your copy() closure
// 5. defer m.EndWrite()                       // atomic.Add(&numberWriters, -1)

// Writer:                          Consumer:
// 1. CAS to reserve cursor    ↔   1. CAS to claim processed range
// 2. Write payload            ↔   2. CAS to advance read pointer
// 3. CAS to mark "ready"      ↔   3. CAS to update writer count
// 4. atomic.Add(&writers, +1) ↔   4. atomic.Add(&writers, -1)

// perf stat -e cache-misses

// # CPU profile
// go test -run=^$ -bench=BenchmarkIngestor_Noop_Parallel -cpuprofile=cpu.prof -parallel=16

// # Memory profile
// go test -run=^$ -bench=BenchmarkIngestor_Noop_Parallel \
//   -memprofile=mem.prof -parallelism=16

// # Analyze
// go tool pprof -top cpu.prof
// go tool pprof -top -alloc_space mem.prof

// go test -run '^$' -bench '^BenchmarkIngestor_Noop_Parallel$' -benchmem

// BenchmarkIngestor_Noop_Parallel-16    	32187890	        39.11 ns/op	       0 B/op	       0 allocs/op
func BenchmarkIngestor_Noop_Parallel(b *testing.B) {
	writer := helpers.NoopWriter{}

	ingestor, err := NewIngestor(
		Size4M(),
		&writer,
	)
	require.NoError(b, err)
	require.NotNil(b, ingestor)

	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	// Warmup: let consumer stabilize
	time.Sleep(10 * time.Millisecond)

	b.ReportAllocs()
	b.SetParallelism(16)
	b.ResetTimer()

	var staticPayload [32]byte

	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				copy(
					staticPayload[:],
					[]byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
				)

				ingestor.write(
					32,
					func(dst []byte) {
						copy(dst, staticPayload[:]) // array doesn't escape
					},
				)
			}
		},
	)

	cancel()
	<-chIngestionEnd
}

// go test -run '^$' -bench '^BenchmarkIngestor_Noop_WriteOnly_FastPath$' -benchmem

// BenchmarkIngestor_Noop_WriteOnly_FastPath-16    	89049547	        12.11 ns/op	       0 B/op	       0 allocs/op
func BenchmarkIngestor_Noop_WriteOnly_FastPath(b *testing.B) {
	writer := helpers.NoopWriter{}
	ingestor, _ := NewIngestor(Size4M(), &writer)

	// Start consumer so writes don't block
	ctx, cancel := context.WithCancel(context.Background())
	chIngestionEnd := ingestor.StartIngestion(ctx)

	// Warmup: let consumer stabilize
	time.Sleep(10 * time.Millisecond)

	b.ReportAllocs()
	b.ResetTimer()

	var staticPayload [32]byte

	for i := 0; i < b.N; i++ {
		copy(
			staticPayload[:],
			[]byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
		)

		ingestor.write(
			32,
			func(dst []byte) {
				copy(dst, staticPayload[:]) // array doesn't escape
			},
		)
	}

	cancel()
	// Don't wait for consumer to finish - we only measured writes

	// Wait for consumer shutdown flush.
	<-chIngestionEnd
}

// go test -run '^$' -bench '^BenchmarkIngestor_Noop_Parallel_Custom$' -benchmem

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:1-16            77928956                15.35 ns/op              1.000 CAS/op         32 B/op          0 allocs/op
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:2-16            15718856                74.80 ns/op              1.000 CAS/op         32 B/op          0 allocs/op
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:6-16            17522360                68.81 ns/op              1.000 CAS/op         32 B/op          0 allocs/op
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:12-16           21093520                55.83 ns/op              1.019 CAS/op         32 B/op          0 allocs/op

// without CAS tracking
// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:1-16         	90904914	        13.04 ns/op	         0 CAS/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:2-16         	19169032	        62.79 ns/op	         0 CAS/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:6-16         	22707710	        54.67 ns/op	         0 CAS/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:12-16        	32791422	        38.33 ns/op	         0 CAS/op	       0 B/op	       0 allocs/op
func BenchmarkIngestor_Noop_Parallel_Custom(b *testing.B) {
	noP := []int{1, 2, 6, 12}

	for _, parallel := range noP {
		b.Run(
			fmt.Sprintf("parallel:%d", parallel),
			func(b *testing.B) {
				writer := helpers.NoopWriter{}
				ingestor, _ := NewIngestor(Size4M(), &writer)

				ctx, cancel := context.WithCancel(context.Background())
				chIngestionEnd := ingestor.StartIngestion(ctx)

				time.Sleep(10 * time.Millisecond) // warmup

				b.ReportAllocs()
				b.ResetTimer()

				helpers.BenchmarkParallel(
					b,
					parallel,
					b.N,
					func() error {
						var payload [32]byte

						ingestor.write(
							32,
							func(dst []byte) {
								copy(dst, payload[:])
							},
						)

						return nil
					},
				)

				// Stop ingestion BEFORE reading metrics
				cancel()
				<-chIngestionEnd

				// Now metrics are stable
				cas := ingestor.Metrics.NumberCAS.Load()
				b.ReportMetric(float64(cas)/float64(b.N), "CAS/op")
			},
		)
	}
}
