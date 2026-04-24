package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"

	_ "unsafe"
)

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena500K-16         	92112229	        12.65 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena500K-16         	79682629	        13.95 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena500K-16        	68676219	        16.88 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena500K-16       	42497878	        27.02 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena1M-16           	85307738	        13.52 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena1M-16           	82265724	        14.27 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena1M-16          	64085607	        17.66 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena1M-16         	41840644	        27.73 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena2M-16           	80781772	        13.31 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena2M-16           	83094717	        14.02 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena2M-16          	67184761	        16.98 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena2M-16         	45147903	        26.53 ns/op	       0 B/op	       0 allocs/op

// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena500K-16         	22969916	        52.62 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena500K-16         	20603295	        56.70 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena500K-16        	20969680	        55.37 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena500K-16       	24698155	        48.68 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena1M-16           	22359103	        52.67 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena1M-16           	20956827	        56.65 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena1M-16          	20993637	        56.39 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena1M-16         	25447803	        47.68 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena2M-16           	21941104	        52.54 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena2M-16           	21190464	        56.80 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena2M-16          	21357494	        56.24 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena2M-16         	25422028	        46.56 ns/op	       0 B/op	       0 allocs/op

// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena500K-16         	23346560	        50.86 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena500K-16         	22507947	        52.61 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena500K-16        	23144041	        51.52 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena500K-16       	26671896	        44.95 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena1M-16           	23499807	        50.88 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena1M-16           	22016530	        53.12 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena1M-16          	22601294	        51.61 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena1M-16         	27335908	        42.20 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena2M-16           	23069248	        50.91 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena2M-16           	22575738	        52.99 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena2M-16          	22999663	        51.11 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena2M-16         	28422356	        42.00 ns/op	       0 B/op	       0 allocs/op

// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena500K-16         	27401814	        46.69 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena500K-16         	25608570	        46.98 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena500K-16        	26206598	        45.80 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena500K-16       	29031273	        40.62 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena1M-16           	25388796	        46.59 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena1M-16           	25336291	        47.10 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena1M-16          	26006442	        45.71 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena1M-16         	30401931	        38.82 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena2M-16           	25559407	        46.65 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena2M-16           	25735082	        47.05 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena2M-16          	26310469	        45.23 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena2M-16         	31982463	        37.40 ns/op	       0 B/op	       0 allocs/op

// go test -run '^$' -bench '^BenchmarkIngestor_MultipleSizes$' -benchmem

func BenchmarkIngestor_MultipleSizes(b *testing.B) {
	sizesMessage := []int{16, 64, 256, 1024}
	sizesArena := []Size{Size500K, Size1M, Size2M}
	gomaxprocsValues := []int{1, 2, 3, 4}

	for _, g := range gomaxprocsValues {
		fmt.Println("")

		for _, sizeArena := range sizesArena {
			for _, sizeMessage := range sizesMessage {
				b.Run(
					fmt.Sprintf(
						"gomaxprocs%d_size_msg%d_arena%s",
						g,
						sizeMessage,
						sizeArena.String(),
					),

					func(b *testing.B) {
						prev := runtime.GOMAXPROCS(g)
						defer runtime.GOMAXPROCS(prev)

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

						b.SetParallelism(16)

						b.RunParallel(
							func(pb *testing.PB) {
								for pb.Next() {
									_, _ = ingestor.Write(payload)
								}
							},
						)

						cancel()
						<-chIngestionEnd
					},
				)
			}
		}
	}
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}

	idx := int(float64(len(sorted)-1) * p)

	if idx < 0 {
		idx = 0
	}

	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}

	return sorted[idx]
}

// BenchmarkIngestor_LatencyHistogram measures tail latency of the ingestor pipeline
// under parallel load with controlled warmup and monotonic timing.
//
// ═══════════════════════════════════════════════════════════════════════════
// MEASUREMENT METHODOLOGY
// ═══════════════════════════════════════════════════════════════════════════
//
// 1. TIME SOURCE: helpers.Nanotime() (monotonic, ~21ns overhead)
//
//   - NOT time.Now(): avoids wall-clock adjustments and reduces measurement noise
//
//   - Deltas are comparable to real time; absolute values are process-relative
//
//     2. WARMUP STRATEGY: Two-phase to eliminate cold-start bias
//     a) Global: time.Sleep(10ms) before ResetTimer() — lets runtime/GC settle
//     b) Per-goroutine: 20ms time-based discard inside RunParallel
//
//   - Each worker skips samples taken before (Nanotime() + 20ms)
//
//   - Ensures ALL workers exclude cold-start, not just one (CAS bug fix)
//
// 3. PARALLELISM: b.SetParallelism(16) with configurable GOMAXPROCS
//   - Simulates concurrent writers competing for ingestor resources
//   - Tail latency (p99.9) reveals contention/GC effects invisible at p50
//
// 4. SAMPLE COLLECTION: Per-goroutine local buffers, merged under mutex
//   - Avoids lock contention during hot path
//   - Final histogram sorted for percentile calculation
//
// 5. GC AWARENESS: Outliers at p99.9 are often GC pauses, not code slowness
//   - Core path (p50-p99) is allocation-free; GC affects tail via stop-the-world
//   - Use GOGC tuning or GC-off diagnostics to isolate algorithmic latency
//
// ═══════════════════════════════════════════════════════════════════════════
// DIAGNOSTIC COMMANDS (output spooled to root/analysis_*.out)
// ═══════════════════════════════════════════════════════════════════════════
//
// # Baseline: default GC, parallel=16
// go test -bench=BenchmarkIngestor_LatencyHistogram -run=^$ -count=5 -benchtime=1s ./... > analysis_baseline.out 2>&1
//
// # Isolate contention: single parallel worker
// go test -bench=BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024 -run=^$ -parallel=1 -count=5 -benchtime=1s ./... > analysis_parallel1.out 2>&1
//
// # Confirm GC is outlier source: disable automatic GC (diagnostic only)
// GOGC=off go test -bench=BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024 -run=^$ -count=5 -benchtime=1s ./... > analysis_gc_off.out 2>&1
//
// # Production-like tuning: reduce GC frequency (trade memory for latency)
// GOGC=150 GOMEMLIMIT=4GiB go test -bench=BenchmarkIngestor_LatencyHistogram -run=^$ -count=5 -benchtime=1s ./... > analysis_gogc150.out 2>&1
//
// # Trace GC pauses: correlate with latency outliers
// GODEBUG=gctrace=1 go test -bench=BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024 -run=^$ -count=3 -benchtime=1s ./... > analysis_gctrace.out 2>&1
//
// # Combined trace: GC + scheduler events (verbose, use sparingly)
// GODEBUG=gctrace=1,schedtrace=10ms go test -bench=BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024 -run=^$ -count=1 -benchtime=2s ./... > analysis_combined.out 2>&1
//
// ═══════════════════════════════════════════════════════════════════════════
// RESULT INTERPRETATION
// ═══════════════════════════════════════════════════════════════════════════
//
// EXPECTED RANGES (1KB message, GOMAXPROCS=1, AMD Ryzen 7 5800H):
//
//	p50:   40–60 ns  → core algorithmic latency (allocation-free path)
//	p99:   70–100 ns → minor contention, cache effects
//	p99.9: 4–5 µs    → true noise floor (GC disabled)
//	p99.9: 10–20 µs  → tuned GC (GOGC=150)
//	p99.9: 100–900 µs → default GC (pause intersection)
//
// IF p99.9 IS HIGH:
//  1. Check analysis_gc_off.out: if p99.9 drops to ~5µs → GC is the cause
//  2. Check analysis_gctrace.out: look for "gc X @... ms" lines near outlier timestamps
//  3. If outliers persist with GC off → investigate lock contention or arena edge cases
//
// REPORTING GUIDANCE:
//   - For algorithmic performance: cite p50/p99 from GC-off or tuned-GC runs
//   - For production SLAs: cite p99.9 from GOGC=150 run (realistic tail)
//   - Always disclose GC configuration: "p99.9: 12µs (GOGC=150), noise floor: 5µs"
//
// ═══════════════════════════════════════════════════════════════════════════
// SEQUENCE OF OPERATIONS (per sub-benchmark)
// ═══════════════════════════════════════════════════════════════════════════
//
// [SETUP]
//  1. Set GOMAXPROCS(g) for this sub-benchmark
//  2. Create ingestor with arena size + noop writer
//  3. Start ingestion goroutine with context
//  4. Prepare payload (repeated bytes)
//
// [WARMUP]
//  5. Global: time.Sleep(10ms) — runtime/GC settling
//  6. b.ReportAllocs(), b.ResetTimer(), b.SetParallelism(16)
//
// [MEASUREMENT LOOP — per parallel worker]
//  7. Record warmup deadline: warmupUntil = Nanotime() + 20ms
//  8. FOR each iteration (pb.Next()):
//     a. start = Nanotime()
//     b. _, _ = ingestor.Write(payload)  // measured operation
//     c. elapsed = Nanotime() - start
//     d. IF start >= warmupUntil: append elapsed to local buffer
//  9. AFTER loop: merge local buffer into global slice (under mutex)
//
// [CLEANUP]
//  10. cancel() context, wait for ingestion to finish (<-chIngestionEnd)
//  11. Merge all worker buffers, sort latencies
//  12. Keep histogram from largest b.N (benchmark convergence)
//  13. Compute and log percentiles: p50, p90, p99, p99.9
//
// ═══════════════════════════════════════════════════════════════════════════

func BenchmarkIngestor_LatencyHistogram(b *testing.B) {
	gomaxprocsValues := []int{1, 2}
	sizesMessage := []int{256, 1024}
	sizesArena := []Size{Size1M}

	for _, g := range gomaxprocsValues {
		for _, sizeArena := range sizesArena {
			for _, sizeMessage := range sizesMessage {
				b.Run(
					fmt.Sprintf(
						"gomaxprocs%d_size_msg%d_arena%s",
						g,
						sizeMessage,
						sizeArena.String(),
					),
					func(b *testing.B) {
						prev := runtime.GOMAXPROCS(g)
						defer runtime.GOMAXPROCS(prev)

						ingestor, errCrIngestor := NewIngestor(
							sizeArena(),
							&helpers.NoopWriter{},
						)
						require.NoError(b, errCrIngestor)
						require.NotNil(b, ingestor)

						ctx, cancel := context.WithCancel(context.Background())
						chIngestionEnd := ingestor.StartIngestion(ctx)

						payload := bytes.Repeat([]byte("x"), sizeMessage)

						var (
							all []int64
							mu  sync.Mutex

							bestN         int
							bestHistogram []int64
						)

						// Global warmup: let runtime settle before measurements begin
						time.Sleep(10 * time.Millisecond)

						b.ReportAllocs()
						b.ResetTimer()
						b.SetParallelism(16)

						b.RunParallel(
							func(pb *testing.PB) {
								local := make([]int64, 0, 4096)

								// FIX: Use monotonic nanotime for warmup window
								// helpers.Nanotime() returns absolute monotonic ns (like runtime.nanotime)
								// Deltas are comparable to real time, even if epoch differs.
								warmupUntil := helpers.Nanotime() + 20*1e6 // 20ms in nanoseconds

								for pb.Next() {
									start := helpers.Nanotime()
									_, _ = ingestor.Write(payload)
									elapsed := helpers.Nanotime() - start // delta in nanoseconds

									// Only record samples taken AFTER the warmup window
									if start >= warmupUntil {
										local = append(local, elapsed)
									}
								}

								if len(local) == 0 {
									return
								}

								mu.Lock()

								all = append(all, local...)
								mu.Unlock()
							},
						)

						cancel()
						<-chIngestionEnd

						if len(all) == 0 {
							return
						}

						merged := make([]int64, 0, len(all))

						merged = append(merged, all...)

						if len(merged) == 0 {
							return
						}

						slices.Sort(merged)

						// Keep only the histogram from the largest b.N
						if b.N > bestN {
							bestN = b.N
							bestHistogram = merged
						}

						// Print only after the final repetition
						if b.N == bestN {
							p50 := percentile(bestHistogram, 0.50)
							p90 := percentile(bestHistogram, 0.90)
							p99 := percentile(bestHistogram, 0.99)
							p999 := percentile(bestHistogram, 0.999)

							b.Logf("latency ns: p50=%d p90=%d p99=%d p99.9=%d",
								p50, p90, p99, p999,
							)
						}
					},
				)
			}
		}
	}
}
