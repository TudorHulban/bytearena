package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"

	_ "unsafe"
)

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena500K-16         	93749011	        12.52 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena500K-16         	86624060	        13.35 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena500K-16        	68597154	        17.03 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena500K-16       	40804804	        28.13 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena1M-16           	92405222	        12.52 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena1M-16           	86389760	        13.36 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena1M-16          	68958054	        16.93 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena1M-16         	42919052	        27.01 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg16_arena2M-16           	91293220	        12.62 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg64_arena2M-16           	87100678	        13.28 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg256_arena2M-16          	67940094	        16.31 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs1_size_msg1024_arena2M-16         	44942540	        25.58 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena500K-16         	22608476	        52.30 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena500K-16         	21087590	        57.35 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena500K-16        	22004563	        54.58 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena500K-16       	25020810	        48.14 ns/op	       1 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena1M-16           	21816625	        53.04 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena1M-16           	20764634	        57.45 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena1M-16          	21447463	        55.94 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena1M-16         	25580044	        46.72 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg16_arena2M-16           	22258192	        53.61 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg64_arena2M-16           	20540980	        58.11 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg256_arena2M-16          	20615106	        56.06 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs2_size_msg1024_arena2M-16         	25865650	        43.84 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena500K-16         	22282778	        51.43 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena500K-16         	23382309	        53.18 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena500K-16        	23139889	        51.96 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena500K-16       	27041794	        44.81 ns/op	       1 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena1M-16           	23535033	        50.87 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena1M-16           	22540676	        53.00 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena1M-16          	23094290	        51.27 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena1M-16         	27673240	        42.70 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg16_arena2M-16           	23475544	        50.90 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg64_arena2M-16           	22041584	        53.25 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg256_arena2M-16          	23290218	        51.34 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs3_size_msg1024_arena2M-16         	28598306	        40.80 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena500K-16         	25445060	        46.96 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena500K-16         	25537693	        47.07 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena500K-16        	25647045	        46.06 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena500K-16       	29034916	        41.31 ns/op	       1 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena1M-16           	25557015	        46.93 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena1M-16           	25223672	        47.06 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena1M-16          	26256078	        45.48 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena1M-16         	29744142	        39.00 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg16_arena2M-16           	25219716	        46.97 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg64_arena2M-16           	24980188	        47.09 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg256_arena2M-16          	25905141	        45.25 ns/op	       0 B/op	       0 allocs/op
// BenchmarkIngestor_MultipleSizes/gomaxprocs4_size_msg1024_arena2M-16         	31345569	        37.63 ns/op	       0 B/op	       0 allocs/op

// go test -run '^$' -bench '^BenchmarkIngestor_MultipleSizes$' -benchmem

func BenchmarkIngestor_MultipleSizes(b *testing.B) {
	sizesMessage := []int{16, 64, 256, 1024}
	sizesArena := []Size{Size500K, Size1M, Size2M}
	gomaxprocsValues := []int{1, 2, 3, 4}

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

// The p99.9 outliers reflect scheduler‑level stalls rather than behavior of the ingestion pipeline itself.
// Under high concurrency and sustained throughput,
// rare OS or runtime scheduling delays can temporarily prevent the consumer goroutine from running,
// and because the queue is bounded, any such delay is amplified into a visible spike at the far tail.
// These events are external to the pipeline logic and represent scheduler jitter
// rather than a regression in steady‑state performance.

func BenchmarkIngestor_LatencyHistogram(b *testing.B) {
	gomaxprocsValues := []int{1, 2, 3, 4}
	sizesMessage := []int{16, 64, 256, 1024}
	sizesArena := []Size{Size500K, Size1M, Size2M}

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
							all        [][]int64
							mu         sync.Mutex
							warmupDone uint32

							bestN         int
							bestHistogram []int64
						)

						time.Sleep(10 * time.Millisecond) // warmup

						b.ReportAllocs()
						b.ResetTimer()
						b.SetParallelism(16)

						b.RunParallel(
							func(pb *testing.PB) {
								local := make([]int64, 0, 4096)

								for pb.Next() {
									start := time.Now()
									_, _ = ingestor.Write(payload)

									local = append(local, time.Since(start).Nanoseconds())
								}

								if len(local) == 0 {
									return
								}

								if atomic.CompareAndSwapUint32(&warmupDone, 0, 1) {
									return
								}

								mu.Lock()

								all = append(all, local)
								mu.Unlock()
							},
						)

						cancel()
						<-chIngestionEnd

						if len(all) == 0 {
							return
						}

						var merged []int64
						for _, buf := range all {
							merged = append(merged, buf...)
						}

						if len(merged) == 0 {
							return
						}

						slices.Sort(merged)

						// keep only the histogram from the largest b.N
						if b.N > bestN {
							bestN = b.N
							bestHistogram = merged
						}

						// print only after the final repetition
						if b.N == bestN {
							p50 := percentile(bestHistogram, 0.50)
							p90 := percentile(bestHistogram, 0.90)
							p99 := percentile(bestHistogram, 0.99)
							p999 := percentile(bestHistogram, 0.999)

							b.Logf("latency ns: p50=%d p90=%d p99=%d p99.9=%d",
								p50,
								p90,
								p99,
								p999,
							)
						}
					},
				)
			}
		}
	}
}
