package bytearena

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

func Benchmark_Overhead_Demo(b *testing.B) {
	b.SetParallelism(1)
	b.ResetTimer()

	// Test 1: Simple loop (baseline)
	b.Run("SimpleLoop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Empty body
		}
	})

	// Test 2: RunParallel with empty body
	b.Run("RunParallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				// Empty body
			}
		})
	})
}

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

// BenchmarkIngestor_Noop_Parallel-12     13983942                91.65 ns/op           31 B/op          0 allocs/op
func BenchmarkIngestor_Noop_Parallel(b *testing.B) {
	writer := helpers.NoopWriter{}

	ingestor, err := NewIngestor(Size4M(), &writer)
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

// BenchmarkIngestor_Noop_WriteOnly_FastPath-12            92831403                12.66 ns/op           32 B/op          0 allocs/op
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

// P=1 (2 goroutines total):
// ├─ Writer: CAS succeeds ~95% of first try
// ├─ Consumer: Updates cursors independently most of the time
// └─ Result: ~13ns (near hardware floor)

// P=2 (3 goroutines total):
// ├─ Writer A: CAS succeeds ~50% first try
// ├─ Writer B: CAS succeeds ~50% first try
// ├─ Consumer: Still updating same cache lines
// └─ Result: ~62ns (cache-line bouncing + retries)

// P=6 (7 goroutines total):
// ├─ All writers: Spend most time spinning/waiting
// ├─ CAS success rate per attempt: ~15%
// ├─ But: Only ONE writer does useful work per cycle anyway
// └─ Result: ~82ns (saturation — adding more writers doesn't hurt much more)

// Base write cost: ~14 ns
// + 0.28 extra CAS × ~80 ns cache bounce = ~22 ns
// + Memory barriers × 3 goroutines = ~30 ns
// + Scheduler jitter = ~10 ns
// ─────────────────────────────
// Total: ~76 ns  ← Matches 79.67 ns!

// cpu: AMD Ryzen 5 5600U with Radeon Graphics
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:1-12            84114493                13.24 ns/op           32 B/op          0 allocs/op
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:2-12            19078651                62.16 ns/op           31 B/op          0 allocs/op
// BenchmarkIngestor_Noop_Parallel_Custom/parallel:6-12            14818225                81.91 ns/op           31 B/op          0 allocs/op
func BenchmarkIngestor_Noop_Parallel_Custom(b *testing.B) {
	noP := []int{1, 2, 6}

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
