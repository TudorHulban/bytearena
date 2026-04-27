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
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena500K-16         	92229417	        12.53 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena500K-16         	86429787	        13.30 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena500K-16        	71975257	        16.37 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena500K-16       	45828294	        27.08 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena1M-16           	92814968	        12.61 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena1M-16           	86055390	        13.50 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena1M-16          	67051242	        17.24 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena1M-16         	43304878	        27.42 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena2M-16           	94758631	        12.56 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena2M-16           	88774165	        13.33 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena2M-16          	69821870	        16.48 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena2M-16         	46877703	        26.19 ns/op	       0 B/op	       0 allocs/op

// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena500K-16         	22853656	        51.91 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena500K-16         	21227962	        55.32 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena500K-16        	21554841	        55.06 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena500K-16       	25082690	        44.32 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena1M-16           	23140822	        51.62 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena1M-16           	21333146	        56.63 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena1M-16          	21333662	        53.93 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena1M-16         	25986260	        45.52 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena2M-16           	23075481	        51.22 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena2M-16           	21069613	        56.95 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena2M-16          	21100280	        55.47 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena2M-16         	27057748	        42.96 ns/op	       0 B/op	       0 allocs/op

// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena500K-16         	23304505	        50.78 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena500K-16         	21876628	        52.60 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena500K-16        	23044642	        51.26 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena500K-16       	26887562	        44.60 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena1M-16           	23624174	        51.08 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena1M-16           	22417822	        51.11 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena1M-16          	23181934	        51.35 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena1M-16         	27693124	        42.26 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena2M-16           	23686021	        50.60 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena2M-16           	22384056	        53.07 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena2M-16          	23441305	        51.19 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena2M-16         	28637572	        41.11 ns/op	       0 B/op	       0 allocs/op

// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena500K-16         	26166066	        46.16 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena500K-16         	25621362	        46.51 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena500K-16        	26015052	        45.35 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena500K-16       	29597569	        40.25 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena1M-16           	25630144	        46.36 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena1M-16           	25141058	        46.63 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena1M-16          	26375868	        44.72 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena1M-16         	30625490	        38.59 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena2M-16           	25812825	        46.13 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena2M-16           	25723315	        45.88 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena2M-16          	26695551	        44.89 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena2M-16         	32059330	        37.15 ns/op	       0 B/op	       0 allocs/op

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

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena500K-16         	22472245	        53.78 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=24 p90=31 p99=57 p99.9=63
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=24 p90=31 p99=58 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena500K-16         	21815481	        54.97 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=25 p90=36 p99=61 p99.9=63
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=25 p90=39 p99=61 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena500K-16        	20290210	        59.66 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=30 p90=57 p99=63 p99.9=99
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=33 p90=58 p99=63 p99.9=108
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena500K-16       	17173174	        71.94 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=47 p90=61 p99=87 p99.9=416929
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=48 p90=61 p99=103 p99.9=660100
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena1M-16           	22160406	        55.20 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=24 p90=33 p99=60 p99.9=63
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=25 p90=45 p99=62 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena1M-16           	21371499	        56.14 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=26 p90=51 p99=62 p99.9=63
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=26 p90=50 p99=62 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena1M-16          	19587306	        61.56 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=39 p90=59 p99=63 p99.9=124
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=39 p90=59 p99=63 p99.9=124
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena1M-16         	16449535	        73.40 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=48 p90=62 p99=116 p99.9=7281
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=48 p90=62 p99=117 p99.9=4025
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena2M-16           	22551214	        53.92 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg16_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=24 p90=32 p99=59 p99.9=61
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=24 p90=31 p99=59 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena2M-16           	21567256	        55.36 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg64_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=25 p90=44 p99=62 p99.9=63
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=25 p90=45 p99=62 p99.9=63
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena2M-16          	19803994	        60.63 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg256_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=36 p90=58 p99=63 p99.9=117
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=37 p90=58 p99=63 p99.9=119
// BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena2M-16         	16468500	        72.64 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs1_size_msg1024_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=48 p90=62 p99=116 p99.9=276
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=48 p90=62 p99=116 p99.9=368

// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena500K-16         	23036830	        52.29 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=86 p90=122 p99=218 p99.9=254
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=85 p90=122 p99=222 p99.9=370
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena500K-16         	20047348	        61.15 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=98 p90=126 p99=242 p99.9=487
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=98 p90=126 p99=243 p99.9=503
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena500K-16        	20615682	        58.86 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=93 p90=124 p99=237 p99.9=911979
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=94 p90=124 p99=237 p99.9=1042943
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena500K-16       	18743623	        62.64 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=97 p90=125 p99=251 p99.9=477124
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=96 p90=125 p99=249 p99.9=484733
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena1M-16           	23322960	        52.90 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=83 p90=121 p99=220 p99.9=253
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=86 p90=122 p99=222 p99.9=282
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena1M-16           	20105034	        59.89 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=98 p90=126 p99=242 p99.9=384
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=97 p90=126 p99=241 p99.9=416
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena1M-16          	20819817	        58.76 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=91 p90=125 p99=240 p99.9=816
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=91 p90=125 p99=242 p99.9=925
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena1M-16         	19553530	        61.13 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=97 p90=125 p99=245 p99.9=799998
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=96 p90=124 p99=242 p99.9=814586
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena2M-16           	23323413	        52.59 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg16_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=82 p90=124 p99=253 p99.9=1000
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=86 p90=122 p99=221 p99.9=359
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena2M-16           	20387143	        60.71 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg64_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=98 p90=127 p99=242 p99.9=332
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=98 p90=126 p99=242 p99.9=440
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena2M-16          	20709927	        59.44 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg256_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=93 p90=124 p99=235 p99.9=499
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=92 p90=126 p99=244 p99.9=701
// BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena2M-16         	19695610	        61.74 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs2_size_msg1024_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=97 p90=132 p99=246 p99.9=1003267
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=97 p90=137 p99=247 p99.9=1066984

// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena500K-16         	23787738	        48.16 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=117 p90=225 p99=253 p99.9=701
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=113 p90=221 p99=253 p99.9=510
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena500K-16         	23320734	        51.12 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=117 p90=224 p99=253 p99.9=528
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=116 p90=224 p99=253 p99.9=843
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena500K-16        	22745233	        52.85 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=121 p90=228 p99=254 p99.9=997924
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=125 p90=230 p99=254 p99.9=1311054
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena500K-16       	22786059	        54.25 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=110 p90=218 p99=449 p99.9=477787
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=113 p90=223 p99=500 p99.9=486848
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena1M-16           	23809051	        50.83 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=126 p90=229 p99=253 p99.9=327
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=119 p90=226 p99=253 p99.9=498
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena1M-16           	24258440	        47.31 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=109 p90=215 p99=254 p99.9=991
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=108 p90=211 p99=252 p99.9=675
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena1M-16          	24303978	        53.09 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=118 p90=226 p99=254 p99.9=5248
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=131 p90=231 p99=254 p99.9=3882
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena1M-16         	23327580	        52.54 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=103 p90=191 p99=253 p99.9=797253
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=111 p90=219 p99=255 p99.9=863791
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena2M-16           	24668467	        50.83 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg16_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=126 p90=230 p99=254 p99.9=819
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=119 p90=226 p99=253 p99.9=510
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena2M-16           	25629139	        47.74 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg64_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=108 p90=212 p99=252 p99.9=420
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=110 p90=215 p99=252 p99.9=542
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena2M-16          	22794222	        52.84 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg256_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=129 p90=231 p99=254 p99.9=1056
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=130 p90=231 p99=254 p99.9=868
// BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena2M-16         	23760114	        51.73 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs3_size_msg1024_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=109 p90=214 p99=253 p99.9=1233519
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=110 p90=217 p99=254 p99.9=1312579

// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena500K-16         	26182554	        46.41 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=166 p90=240 p99=342 p99.9=664
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=168 p90=240 p99=332 p99.9=587
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena500K-16         	25862274	        46.63 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=167 p90=240 p99=328 p99.9=2035
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=169 p90=240 p99=340 p99.9=1287
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena500K-16        	26075492	        46.17 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=159 p90=239 p99=369 p99.9=1172724
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=161 p90=239 p99=391 p99.9=1283565
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena500K-16       	25076586	        47.82 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena500K-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=166 p90=242 p99=6255 p99.9=480427
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=169 p90=242 p99=7481 p99.9=483815
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena1M-16           	26199087	        46.44 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=172 p90=241 p99=390 p99.9=910
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=169 p90=240 p99=334 p99.9=528
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena1M-16           	26750539	        44.92 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=161 p90=238 p99=255 p99.9=503
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=162 p90=238 p99=255 p99.9=760
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena1M-16          	24150784	        46.04 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=183 p90=246 p99=459 p99.9=1380557
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=162 p90=239 p99=371 p99.9=1399246
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena1M-16         	25507531	        46.20 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena1M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=168 p90=242 p99=461 p99.9=831738
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=168 p90=242 p99=464 p99.9=853782
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena2M-16           	26934801	        44.96 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg16_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=161 p90=238 p99=255 p99.9=499
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=162 p90=238 p99=255 p99.9=511
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena2M-16           	26201598	        46.64 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg64_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=168 p90=240 p99=300 p99.9=503
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=169 p90=240 p99=324 p99.9=546
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena2M-16          	25178073	        47.79 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg256_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=170 p90=241 p99=379 p99.9=1648
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=171 p90=241 p99=396 p99.9=1437
// BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena2M-16         	24708711	        46.68 ns/op	       0 B/op	       0 allocs/op
// --- BENCH: BenchmarkIngestor_LatencyHistogram/gomaxprocs4_size_msg1024_arena2M-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=178 p90=244 p99=458 p99.9=1351741
//     /mnt/tmpfs.ramdisk/bytearena/tc_23_multiple_sizes_test.go:364: latency ns: p50=175 p90=243 p99=432 p99.9=1396627
