package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/bits"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"

	_ "unsafe"
)

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena500K-16         	93706886	        12.62 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena500K-16         	86340444	        13.25 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena500K-16        	72286626	        16.50 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena500K-16       	43713616	        26.66 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena1M-16           	91304946	        12.74 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena1M-16           	85316491	        13.64 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena1M-16          	63508874	        17.48 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena1M-16         	43493824	        28.08 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena2M-16           	91590104	        12.66 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena2M-16           	85475953	        13.30 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena2M-16          	69848607	        16.49 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena2M-16         	44444970	        25.76 ns/op	       0 B/op	       0 allocs/op

// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena500K-16         	28916175	        40.37 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena500K-16         	27846868	        36.31 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena500K-16        	27386991	        43.24 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena500K-16       	30282505	        39.55 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena1M-16           	41208444	        40.20 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena1M-16           	28721244	        37.11 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena1M-16          	28281145	        44.35 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena1M-16         	31054844	        38.15 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena2M-16           	28868204	        40.38 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena2M-16           	28721671	        40.52 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena2M-16          	27437534	        42.94 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena2M-16         	32181806	        36.14 ns/op	       0 B/op	       0 allocs/op

// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena500K-16         	29502590	        40.70 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena500K-16         	29238703	        37.28 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena500K-16        	30156598	        39.11 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena500K-16       	35808921	        32.28 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena1M-16           	28884516	        40.93 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena1M-16           	29917196	        38.06 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena1M-16          	29219942	        39.43 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena1M-16         	37562536	        30.53 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena2M-16           	29146850	        40.63 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena2M-16           	29905654	        38.43 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena2M-16          	30028626	        38.71 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena2M-16         	39021520	        31.31 ns/op	       0 B/op	       0 allocs/op

// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena500K-16         	32461923	        36.56 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena500K-16         	33414706	        35.84 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena500K-16        	33372566	        36.09 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena500K-16       	36397053	        31.80 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena1M-16           	32479299	        37.05 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena1M-16           	33745485	        35.94 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena1M-16          	33098322	        35.87 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena1M-16         	38927568	        30.43 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena2M-16           	32036960	        36.70 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena2M-16           	33739938	        35.60 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena2M-16          	33968858	        35.12 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena2M-16         	40598539	        29.04 ns/op	       0 B/op	       0 allocs/op

// go test -run '^$' -bench '^BenchmarkIngestor_MultipleSizes$' -benchmem

func BenchmarkIngestor_MultipleSizes(b *testing.B) {
	gomaxprocsValues := []int{1, 2, 3, 4}
	sizesMessage := []int{16, 64, 256, 1024}
	sizesArena := []Size{Size500K, Size1M, Size2M}

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

							helpers.TernaryWithValueIn(
								[]int{1},
								g,
								nil,
								WithCounterCoreCPU(),
							),
						)
						require.NoError(b, errCrIngestor)
						require.NotNil(b, ingestor)

						ctx, cancel := context.WithCancel(context.Background())
						chIngestionEnd := ingestor.StartIngestion(ctx)

						payload := bytes.Repeat([]byte("x"), sizeMessage)

						runtime.GC()

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

// p99.9 (ns) — Log Scale
//    │
// 100 ├─●──●──●──●──●──●──●──●──●──●──●──●──●──●──●  ← Low tail: 110ns–1.4µs
//     │
// 1K  ├─
//     │
// 10K ├──────────────●──●──●──●──●──●──●──●──●──●──●  ← Transition: 4–10µs
//     │
// 100K├──────────────────────────────────────────────●──●──●──●──●──●──●──●──●──●  ← High tail: 100–900µs
//     │
// 1M  ├─
//     │
// 10M ├────────────────────────────────────────────────────────────────●──●──●──●──●  ← Race detector: 5–9ms (ignore)
//     │
//     └──────────────────────────────────────────────────────────────────────────►
//          baseline  gogc150  gc_off  gctrace  race

// Histogram gives the caller-perceived latency.
// The benchmark's ns/op gives system throughput and might be lower than caller-perceived latency.
func BenchmarkIngestor_LatencyHistogram(b *testing.B) {
	gomaxprocsValues := []int{1, 2, 3, 4}
	sizesMessage := []int{16, 64, 256, 1024}
	sizesArena := []Size{Size500K, Size1M, Size2M}

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

							helpers.TernaryWithValueIn(
								[]int{1},
								g,
								nil,
								WithCounterCoreCPU(),
							),
						)
						require.NoError(b, errCrIngestor)
						require.NotNil(b, ingestor)

						ctx, cancel := context.WithCancel(context.Background())
						chIngestionEnd := ingestor.StartIngestion(ctx)

						payload := bytes.Repeat([]byte("x"), sizeMessage)

						type histogram struct {
							buckets [64]uint64
						}

						var (
							globalHist histogram
							mu         sync.Mutex
						)

						// Global warmup: let runtime settle before measurements begin
						time.Sleep(10 * time.Millisecond)

						runtime.GC()

						b.ReportAllocs()
						b.ResetTimer()
						b.SetParallelism(16)

						b.RunParallel(
							func(pb *testing.PB) {
								var localHist histogram

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
										idx := 0
										if elapsed > 0 {
											idx = bits.Len64(uint64(elapsed)) - 1
											if idx >= len(localHist.buckets) {
												idx = len(localHist.buckets) - 1
											}
										}

										localHist.buckets[idx]++
									}
								}

								mu.Lock()
								for i := range globalHist.buckets {
									globalHist.buckets[i] += localHist.buckets[i]
								}
								mu.Unlock()
							},
						)

						cancel()
						<-chIngestionEnd

						total := uint64(0)
						for _, c := range globalHist.buckets {
							total += c
						}

						if total == 0 {
							return
						}

						p50 := percentileFromHistogram(globalHist.buckets, total, 0.50)
						p90 := percentileFromHistogram(globalHist.buckets, total, 0.90)
						p99 := percentileFromHistogram(globalHist.buckets, total, 0.99)
						p999 := percentileFromHistogram(globalHist.buckets, total, 0.999)

						b.Logf("latency ns: p50=%d p90=%d p99=%d p99.9=%d",
							p50, p90, p99, p999,
						)
					},
				)
			}
		}
	}
}

func percentileFromHistogram(buckets [64]uint64, total uint64, p float64) int64 {
	target := uint64(float64(total) * p)
	if target == 0 {
		target = 1
	}

	var cummulative uint64

	for i, c := range buckets {
		cummulative = cummulative + c

		if cummulative >= target {
			if i == 0 {
				return 1 // bucket 0 holds elapsed == 1 ns
			}

			if i == len(buckets)-1 {
				return int64(1) << i // lower bound of the overflow bucket
			}

			low := int64(1) << i
			high := int64(1) << (i + 1)
			prevCum := cummulative - c
			frac := float64(target-prevCum) / float64(c)

			return low + int64(float64(high-low)*frac)
		}
	}

	return int64(math.MaxInt64)
}

// Results:

// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena500K-16         	22170075	        54.31 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=24 p90=31 p99=59 p99.9=63
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=24 p90=31 p99=60 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena500K-16         	21472090	        55.92 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=26 p90=49 p99=62 p99.9=63
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=26 p90=50 p99=62 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena500K-16        	20442757	        59.13 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=30 p90=56 p99=63 p99.9=106
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=31 p90=57 p99=63 p99.9=111
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena500K-16       	16689092	        72.32 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=48 p90=61 p99=108 p99.9=648373
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=48 p90=61 p99=81 p99.9=599616
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena1M-16           	21910178	        55.69 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=24 p90=31 p99=58 p99.9=63
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=26 p90=49 p99=62 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena1M-16           	21348866	        56.35 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=26 p90=51 p99=62 p99.9=63
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=26 p90=52 p99=62 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena1M-16          	19396561	        61.04 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=39 p90=59 p99=63 p99.9=132
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=38 p90=59 p99=63 p99.9=122
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena1M-16         	16320382	        74.20 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=48 p90=62 p99=116 p99.9=4033
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=48 p90=61 p99=113 p99.9=6081
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena2M-16           	22224694	        54.46 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=24 p90=31 p99=58 p99.9=64
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=25 p90=38 p99=61 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena2M-16           	21486655	        55.83 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=26 p90=49 p99=62 p99.9=63
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=26 p90=49 p99=62 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena2M-16          	19982329	        60.50 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=37 p90=58 p99=63 p99.9=111
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=36 p90=58 p99=63 p99.9=117
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena2M-16         	16402243	        74.08 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=48 p90=62 p99=117 p99.9=218
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=48 p90=62 p99=114 p99.9=332

// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena500K-16         	21013754	        57.90 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=97 p90=124 p99=234 p99.9=450
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=97 p90=125 p99=238 p99.9=378
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena500K-16         	20476906	        57.96 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=98 p90=126 p99=242 p99.9=450
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=96 p90=125 p99=239 p99.9=419
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena500K-16        	19699131	        59.62 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=97 p90=126 p99=242 p99.9=836629
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=96 p90=125 p99=239 p99.9=995709
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena500K-16       	18316155	        65.17 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=98 p90=125 p99=247 p99.9=689546
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=97 p90=125 p99=246 p99.9=725060
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena1M-16           	21419790	        58.82 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=93 p90=123 p99=225 p99.9=252
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=97 p90=125 p99=240 p99.9=379
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena1M-16           	21311310	        59.12 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=96 p90=125 p99=239 p99.9=354
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=97 p90=126 p99=242 p99.9=403
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena1M-16          	19497774	        62.97 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=98 p90=126 p99=244 p99.9=820
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=98 p90=127 p99=245 p99.9=796
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena1M-16         	18270415	        64.88 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=99 p90=127 p99=247 p99.9=966010
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=98 p90=125 p99=243 p99.9=975666
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena2M-16           	21222272	        57.97 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=96 p90=124 p99=229 p99.9=255
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=97 p90=125 p99=237 p99.9=339
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena2M-16           	20775507	        57.76 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=95 p90=125 p99=239 p99.9=255
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=96 p90=125 p99=238 p99.9=343
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena2M-16          	18748914	        61.44 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=100 p90=160 p99=250 p99.9=841
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=98 p90=126 p99=243 p99.9=485
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena2M-16         	18516615	        65.17 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=98 p90=126 p99=242 p99.9=393216
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=99 p90=127 p99=244 p99.9=9102

// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena500K-16         	27370688	        43.37 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=101 p90=173 p99=249 p99.9=549
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=100 p90=155 p99=247 p99.9=460
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena500K-16         	27023709	        45.82 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=104 p90=198 p99=252 p99.9=730
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=105 p90=203 p99=251 p99.9=545
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena500K-16        	25711620	        48.14 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=109 p90=213 p99=253 p99.9=1134814
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=110 p90=216 p99=253 p99.9=1258579
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena500K-16       	25257379	        48.33 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=102 p90=187 p99=324 p99.9=478596
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=102 p90=187 p99=369 p99.9=491973
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena1M-16           	26376252	        43.27 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=106 p90=204 p99=251 p99.9=255
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=100 p90=157 p99=248 p99.9=468
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena1M-16           	28056723	        43.37 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=100 p90=156 p99=247 p99.9=440
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=100 p90=159 p99=248 p99.9=491
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena1M-16          	25597395	        47.99 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=109 p90=214 p99=252 p99.9=989
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=110 p90=215 p99=253 p99.9=1355
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena1M-16         	25234640	        47.69 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=103 p90=191 p99=254 p99.9=840870
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=103 p90=192 p99=255 p99.9=856439
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena2M-16           	27812104	        42.92 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=97 p90=159 p99=248 p99.9=465
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=99 p90=143 p99=246 p99.9=415
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena2M-16           	28507923	        43.89 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=100 p90=154 p99=247 p99.9=448
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=101 p90=170 p99=249 p99.9=474
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena2M-16          	26227231	        46.57 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=105 p90=203 p99=252 p99.9=924
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=105 p90=203 p99=252 p99.9=790
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena2M-16         	25387124	        46.85 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=104 p90=194 p99=253 p99.9=1162512
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=102 p90=183 p99=253 p99.9=1229302

// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena500K-16         	28434782	        43.15 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=160 p90=237 p99=255 p99.9=860
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=157 p90=236 p99=254 p99.9=476
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena500K-16         	27536166	        42.82 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=166 p90=238 p99=254 p99.9=710
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=158 p90=236 p99=254 p99.9=818
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena500K-16        	27530836	        44.27 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=160 p90=238 p99=255 p99.9=1267759
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=160 p90=237 p99=255 p99.9=1373668
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena500K-16       	26755437	        43.16 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=173 p90=244 p99=1766 p99.9=479496
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=139 p90=237 p99=1520 p99.9=473000
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena1M-16           	28104758	        43.37 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=150 p90=235 p99=254 p99.9=491
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=158 p90=236 p99=254 p99.9=492
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena1M-16           	27941148	        43.06 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=156 p90=236 p99=254 p99.9=626
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=156 p90=236 p99=254 p99.9=509
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena1M-16          	27020082	        44.24 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=159 p90=237 p99=255 p99.9=967916
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=159 p90=237 p99=255 p99.9=1251985
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena1M-16         	27914031	        41.56 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=142 p90=237 p99=438 p99.9=796514
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=136 p90=236 p99=432 p99.9=817587
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena2M-16           	28050954	        43.16 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=156 p90=236 p99=254 p99.9=457
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=157 p90=236 p99=254 p99.9=466
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena2M-16           	27412264	        42.60 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=168 p90=239 p99=254 p99.9=492
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=154 p90=236 p99=254 p99.9=470
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena2M-16          	27461364	        44.03 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=157 p90=237 p99=254 p99.9=939
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=158 p90=237 p99=255 p99.9=931
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena2M-16         	29027647	        41.30 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=141 p90=236 p99=407 p99.9=980375
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:378: latency ns: p50=143 p90=236 p99=395 p99.9=1188614
