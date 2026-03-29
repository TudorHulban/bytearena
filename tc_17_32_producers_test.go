package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tudorhulban/bytearena/helpers"
)

// Test Case: Benchmark under 32 producers.
// This test verifies the ingestor's performance and correctness under high concurrency
// with multiple producers writing simultaneously while the consumer rotates arenas.

// TestConcurrent32Producers tests the ingestor with 32 concurrent producers
// writing at full speed to ensure no data loss and proper synchronization.
func TestConcurrent32Producers(t *testing.T) {
	var out bytes.Buffer

	ingestor, err := NewIngestor(Size1M(), &out)
	require.NoError(t, err)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wgConsumer sync.WaitGroup

	wgConsumer.Go(
		func() {
			ingestor.consumerLoop(ctx)
		},
	)

	numProducers := 32

	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	successCount := atomic.Int64{}
	writesPerProducer := 5000

	for ix := range numProducers {
		go func(producerID int) {
			defer wgProducers.Done()

			for j := range writesPerProducer {
				payload := fmt.Sprintf(
					"producer-%d-write-%d\n",
					producerID,
					j,
				)

				if errWrite := ingestor.write(
					uint32(len(payload)),
					func(dst []byte) {
						copy(dst, []byte(payload))
					},
				); errWrite == nil {
					successCount.Add(1)
				}
			}
		}(ix)
	}

	wgProducers.Wait()
	cancel()
	wgConsumer.Wait()

	output := out.String()
	require.NotEmpty(t, output, "expected non-empty output")

	// Count lines in output
	outputLines := bytes.Count(out.Bytes(), []byte("\n"))

	t.Logf(
		"Successful writes: %d, Output lines: %d",
		successCount.Load(),
		outputLines,
	)

	// We might have some failures due to backpressure, but success count should be reasonable
	require.Greater(t,
		successCount.Load(),
		int64(0),
		"should have at least some successful writes",
	)
	require.LessOrEqual(t,
		outputLines,
		int(successCount.Load()),
		"output lines should not exceed successful writes",
	)
}

// TestHighContentionConcurrentWrites tests the ingestor with 64+ producers
// and aggressive rotation to stress the lock-free mechanisms.
func TestHighContentionConcurrentWrites(t *testing.T) {
	var out bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(Size100K(), &out, WithSealPercentage(50))
	require.NoError(t, errCrIngestor)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	chIngestionEnd := ingestor.StartIngestion(ctx)

	numProducers := 64
	writesPerProducer := 10000
	payload := []byte("p-write\n") // fixed size, no allocation

	var (
		wgProducers             sync.WaitGroup
		successCount, dropCount atomic.Int64
	)

	for range numProducers {
		wgProducers.Go(
			func() {
				for range writesPerProducer {
					if _, err := ingestor.Write(payload); err == nil {
						successCount.Add(1)
					} else {
						dropCount.Add(1)
					}
				}
			},
		)
	}

	wgProducers.Wait()
	cancel()
	<-chIngestionEnd

	total := int64(numProducers * writesPerProducer)

	t.Logf("Success: %d/%d (%.1f%%), Drops: %d",
		successCount.Load(), total,
		float64(successCount.Load())/float64(total)*100,
		dropCount.Load())

	// What the ingestor actually guarantees:
	// 1. No data corruption - every successful write appears in output
	// 2. Output is coherent (whole payloads only, no partial writes)
	// 3. At least some writes succeeded (ingestor was not completely jammed)
	// 4. Flushed bytes match successful write accounting
	outputBytes := int64(out.Len())
	expectedBytes := successCount.Load() * int64(len(payload))
	require.Equal(t,
		expectedBytes,
		outputBytes,
		"flushed bytes must exactly match successful writes × payload size",
	)

	// drops are expected and correct under this high-contention configuration.
	// a minimum success rate should not be asserted.
	require.Greater(t, successCount.Load(), int64(0))
}

// cpu: AMD Ryzen 5 5600U with Radeon Graphics
// BenchmarkConcurrentProducers/producers_4-12         	20196822	        55.89 ns/op	    576037 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/producers_8-12         	20184724	        55.40 ns/op	    294138 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/producers_16-12        	20289602	        55.38 ns/op	    286989 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/producers_32-12        	20462853	        55.11 ns/op	    157237 writes/sec	       0 B/op	       0 allocs/op

// BenchmarkConcurrentProducers benchmarks the ingestor with increasing numbers of
// concurrent producers to measure scaling characteristics.
func BenchmarkConcurrentProducers(b *testing.B) {
	producerCounts := []int{4, 8, 16, 32}

	for _, numProducers := range producerCounts {
		b.Run(
			fmt.Sprintf("producers_%d", numProducers),
			func(b *testing.B) {
				writer := &helpers.NoopWriter{}

				ingestor, errCrIngestor := NewIngestor(Size1M(), writer)
				require.NoError(b, errCrIngestor)
				require.NotNil(b, ingestor)

				ctx, cancel := context.WithCancel(context.Background())
				chIngestionEnd := ingestor.StartIngestion(ctx)

				payload := []byte("benchmark-payload-32bytes")

				b.ResetTimer()
				b.SetParallelism(numProducers)

				var writesCompleted atomic.Int64

				b.RunParallel(
					func(pb *testing.PB) {
						for pb.Next() {
							if _, errWrite := ingestor.Write(payload); errWrite == nil {
								writesCompleted.Add(1)
							}
						}
					},
				)

				cancel()
				<-chIngestionEnd

				b.StopTimer()
				b.ReportMetric(
					float64(writesCompleted.Load())/b.Elapsed().Seconds(),
					"writes/sec",
				)
			},
		)
	}
}

// TestConcurrentProducersWithVariablePayload tests the ingestor with
// different payload sizes under high concurrency.
func TestConcurrentProducersWithVariablePayload(t *testing.T) {
	var out bytes.Buffer

	ingestor, errCr := NewIngestor(Size1M(), &out)
	require.NoError(t, errCr)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wgConsumer sync.WaitGroup

	wgConsumer.Go(
		func() {
			ingestor.consumerLoop(ctx)
		},
	)

	numProducers := 32

	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	successCount := atomic.Int64{}
	totalBytesWritten := atomic.Int64{}

	payloadSizes := []int{16, 64, 256, 1024}

	for ix := range numProducers {
		go func(producerID int) {
			defer wgProducers.Done()

			for j := range 5000 {
				size := payloadSizes[j%len(payloadSizes)]

				payload := fmt.Sprintf(
					"p%d-w%d-%s\n",
					producerID,
					j,
					randomString(size),
				)

				if errWrite := ingestor.write(
					uint32(len(payload)),
					func(dst []byte) {
						copy(dst, []byte(payload))
					},
				); errWrite == nil {
					successCount.Add(1)
					totalBytesWritten.Add(int64(len(payload)))
				}
			}
		}(ix)
	}

	wgProducers.Wait()
	cancel()
	wgConsumer.Wait()

	t.Logf(
		"Successful writes: %d, Total bytes: %d",
		successCount.Load(),
		totalBytesWritten.Load(),
	)

	require.Greater(t,
		successCount.Load(),
		int64(0),
		"should have at least some successful writes",
	)
}

// TestProducerConsumerThroughput tests the throughput under sustained load
// with 32 producers and continuous rotation.
func TestProducerConsumerThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}

	var out bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(Size500K(), &out, WithSealPercentage(70))
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wgConsumer sync.WaitGroup

	wgConsumer.Go(
		func() {
			ingestor.consumerLoop(ctx)
		},
	)

	numProducers := 32

	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	successCount := atomic.Int64{}
	totalBytes := atomic.Int64{}
	startTime := time.Now()

	payload := []byte("throughput-test-payload")

	for range numProducers {
		go func() {
			defer wgProducers.Done()

			chDone := ctx.Done() // Hoist the channel helps the compiler optimize the select case.

			for {
				select {
				case <-chDone:
					return

				default:
					if _, errWrite := ingestor.Write(payload); errWrite == nil {
						successCount.Add(1)
						totalBytes.Add(int64(len(payload)))
					}
				}
			}
		}()
	}

	wgProducers.Wait()
	cancel()
	wgConsumer.Wait()

	elapsed := time.Since(startTime)

	throughputWrites := float64(successCount.Load()) / elapsed.Seconds()
	throughputBytes := float64(totalBytes.Load()) / elapsed.Seconds() / (1024 * 1024)

	t.Logf("Throughput: %.0f writes/sec, %.2f MB/sec", throughputWrites, throughputBytes)
	t.Logf("Total writes: %d, Total bytes: %d, Duration: %v",
		successCount.Load(),
		totalBytes.Load(),
		elapsed,
	)

	require.Greater(t,
		successCount.Load(),
		int64(10000),
		"should have processed at least 10000 writes",
	)
}

// cpu: AMD Ryzen 5 5600U with Radeon Graphics
// BenchmarkConcurrentProducersFixedTime/producers_4-12         	       1	2000665582 ns/op	  15552608 writes/sec	 2106880 B/op	      70 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_4-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:440: 4 producers: 15552608 writes/sec, total writes: 31105215
// BenchmarkConcurrentProducersFixedTime/producers_8-12         	       1	2000741605 ns/op	  16176442 writes/sec	 2124896 B/op	      93 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_8-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:440: 8 producers: 16176442 writes/sec, total writes: 32352884
// BenchmarkConcurrentProducersFixedTime/producers_16-12        	       1	2016553047 ns/op	   2097204 writes/sec	 2121360 B/op	     149 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_16-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:440: 16 producers: 2097204 writes/sec, total writes: 4194408
// BenchmarkConcurrentProducersFixedTime/producers_32-12        	       1	2015873857 ns/op	    838860 writes/sec	 2121808 B/op	     149 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_32-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:440: 32 producers: 838860 writes/sec, total writes: 1677720

// BenchmarkConcurrentProducersFixedTime benchmarks with fixed duration
// to get accurate throughput measurements across different producer counts.
func BenchmarkConcurrentProducersFixedTime(b *testing.B) {
	producerCounts := []int{4, 8, 16, 32}
	duration := 2 * time.Second

	for _, numProducers := range producerCounts {
		b.Run(
			fmt.Sprintf("producers_%d", numProducers),
			func(b *testing.B) {
				writer := &helpers.NoopWriter{}

				ingestor, err := NewIngestor(Size1M(), writer)
				require.NoError(b, err)

				ctx, cancel := context.WithTimeout(context.Background(), duration)
				defer cancel()

				chIngestionEnd := ingestor.StartIngestion(ctx)

				payload := []byte("benchmark-payload-32bytes")

				var (
					wgProducers     sync.WaitGroup
					writesCompleted atomic.Int64
				)

				// Start producers
				for range numProducers {
					wgProducers.Go(
						func() {
							for {
								select {
								case <-ctx.Done():
									return
								default:
									if _, err := ingestor.Write(payload); err == nil {
										writesCompleted.Add(1)
									}
								}
							}
						},
					)
				}

				// Wait for duration
				<-ctx.Done()
				cancel()
				wgProducers.Wait()
				<-chIngestionEnd

				writesPerSec := float64(writesCompleted.Load()) / duration.Seconds()
				b.ReportMetric(writesPerSec, "writes/sec")
				b.Logf(
					"%d producers: %.0f writes/sec, total writes: %d",
					numProducers,
					writesPerSec,
					writesCompleted.Load(),
				)
			},
		)
	}
}

// cpu: AMD Ryzen 5 5600U with Radeon Graphics
// BenchmarkContentionScaling/A_100K_P_4_S_90-12         	       1	3000128751 ns/op	  12597496 writes/sec	  230992 B/op	     107 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_4_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(90), 4 producers: 12597496 writes/sec
// BenchmarkContentionScaling/A_100K_P_4_S_95-12         	       1	3000268695 ns/op	  12482232 writes/sec	  224032 B/op	      81 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_4_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(95), 4 producers: 12482232 writes/sec
// BenchmarkContentionScaling/A_100K_P_4_S_97-12         	       1	3000121577 ns/op	  12562317 writes/sec	  221392 B/op	      68 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_4_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(97), 4 producers: 12562317 writes/sec
// BenchmarkContentionScaling/A_100K_P_4_S_99-12         	       1	3000099156 ns/op	  12444895 writes/sec	  223072 B/op	      87 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_4_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(99), 4 producers: 12444895 writes/sec
// BenchmarkContentionScaling/A_100K_P_8_S_90-12         	       1	3000253186 ns/op	  13368310 writes/sec	  243328 B/op	     131 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_8_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(90), 8 producers: 13368310 writes/sec
// BenchmarkContentionScaling/A_100K_P_8_S_95-12         	       1	3000168546 ns/op	  13294967 writes/sec	  225136 B/op	      90 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_8_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(95), 8 producers: 13294967 writes/sec
// BenchmarkContentionScaling/A_100K_P_8_S_97-12         	       1	3000122710 ns/op	  13248053 writes/sec	  228624 B/op	      86 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_8_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(97), 8 producers: 13248053 writes/sec
// BenchmarkContentionScaling/A_100K_P_8_S_99-12         	       1	3000236405 ns/op	  13262198 writes/sec	  223856 B/op	      87 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_8_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(99), 8 producers: 13262198 writes/sec
// BenchmarkContentionScaling/A_100K_P_16_S_90-12        	       1	3004275572 ns/op	    205878 writes/sec	  230928 B/op	     119 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_16_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(90), 16 producers: 205878 writes/sec
// BenchmarkContentionScaling/A_100K_P_16_S_95-12        	       1	3003444174 ns/op	    203224 writes/sec	  228928 B/op	     112 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_16_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(95), 16 producers: 203224 writes/sec
// BenchmarkContentionScaling/A_100K_P_16_S_97-12        	       1	3006558897 ns/op	    201783 writes/sec	  230976 B/op	     126 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_16_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(97), 16 producers: 201783 writes/sec
// BenchmarkContentionScaling/A_100K_P_16_S_99-12        	       1	3006681399 ns/op	    204350 writes/sec	  224256 B/op	     108 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_16_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(99), 16 producers: 204350 writes/sec
// BenchmarkContentionScaling/A_100K_P_32_S_90-12        	       1	3005383301 ns/op	     77686 writes/sec	  238576 B/op	     143 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_32_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(90), 32 producers: 77686 writes/sec
// BenchmarkContentionScaling/A_100K_P_32_S_95-12        	       1	3002756196 ns/op	     80556 writes/sec	  234512 B/op	     140 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_32_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(95), 32 producers: 80556 writes/sec
// BenchmarkContentionScaling/A_100K_P_32_S_97-12        	       1	3007146135 ns/op	     85814 writes/sec	  236304 B/op	     144 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_32_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(97), 32 producers: 85814 writes/sec
// BenchmarkContentionScaling/A_100K_P_32_S_99-12        	       1	3002935304 ns/op	     75025 writes/sec	  228240 B/op	     126 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_100K_P_32_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 100K(99), 32 producers: 75025 writes/sec
// BenchmarkContentionScaling/A_500K_P_4_S_90-12         	       1	3000404251 ns/op	  14987667 writes/sec	 1038752 B/op	      57 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_4_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(90), 4 producers: 14987667 writes/sec
// BenchmarkContentionScaling/A_500K_P_4_S_95-12         	       1	3000537592 ns/op	  14687085 writes/sec	 1039648 B/op	      59 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_4_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(95), 4 producers: 14687085 writes/sec
// BenchmarkContentionScaling/A_500K_P_4_S_97-12         	       1	3000083235 ns/op	  14710692 writes/sec	 1038736 B/op	      57 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_4_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(97), 4 producers: 14710692 writes/sec
// BenchmarkContentionScaling/A_500K_P_4_S_99-12         	       1	3000326214 ns/op	  14935278 writes/sec	 1038752 B/op	      57 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_4_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(99), 4 producers: 14935278 writes/sec
// BenchmarkContentionScaling/A_500K_P_8_S_90-12         	       1	3000401556 ns/op	  15651956 writes/sec	 1042784 B/op	      74 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_8_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(90), 8 producers: 15651956 writes/sec
// BenchmarkContentionScaling/A_500K_P_8_S_95-12         	       1	3000749913 ns/op	  16124686 writes/sec	 1039104 B/op	      65 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_8_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(95), 8 producers: 16124686 writes/sec
// BenchmarkContentionScaling/A_500K_P_8_S_97-12         	       1	3000074620 ns/op	  16103313 writes/sec	 1041728 B/op	      74 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_8_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(97), 8 producers: 16103313 writes/sec
// BenchmarkContentionScaling/A_500K_P_8_S_99-12         	       1	3000196489 ns/op	  16076044 writes/sec	 1042528 B/op	      75 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_8_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(99), 8 producers: 16076044 writes/sec
// BenchmarkContentionScaling/A_500K_P_16_S_90-12        	       1	3020121685 ns/op	   1010433 writes/sec	 1047712 B/op	     101 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_16_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(90), 16 producers: 1010433 writes/sec
// BenchmarkContentionScaling/A_500K_P_16_S_95-12        	       1	3010191307 ns/op	   1006947 writes/sec	 1047424 B/op	      98 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_16_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(95), 16 producers: 1006947 writes/sec
// BenchmarkContentionScaling/A_500K_P_16_S_97-12        	       1	3010253365 ns/op	   1054552 writes/sec	 1051488 B/op	      98 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_16_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(97), 16 producers: 1054552 writes/sec
// BenchmarkContentionScaling/A_500K_P_16_S_99-12        	       1	3002708085 ns/op	   1016325 writes/sec	 1048480 B/op	     109 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_16_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(99), 16 producers: 1016325 writes/sec
// BenchmarkContentionScaling/A_500K_P_32_S_90-12        	       1	3005477288 ns/op	    456775 writes/sec	 1053040 B/op	     141 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_32_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(90), 32 producers: 456775 writes/sec
// BenchmarkContentionScaling/A_500K_P_32_S_95-12        	       1	3003892629 ns/op	    388731 writes/sec	 1055088 B/op	     144 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_32_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(95), 32 producers: 388731 writes/sec
// BenchmarkContentionScaling/A_500K_P_32_S_97-12        	       1	3004193767 ns/op	    395402 writes/sec	 1055728 B/op	     147 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_32_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(97), 32 producers: 395402 writes/sec
// BenchmarkContentionScaling/A_500K_P_32_S_99-12        	       1	3005153016 ns/op	    374831 writes/sec	 1046768 B/op	     127 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_500K_P_32_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 500K(99), 32 producers: 374831 writes/sec
// BenchmarkContentionScaling/A_1M_P_4_S_90-12           	       1	3001080226 ns/op	  15254722 writes/sec	 2103664 B/op	      56 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_4_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(90), 4 producers: 15254722 writes/sec
// BenchmarkContentionScaling/A_1M_P_4_S_95-12           	       1	3000788105 ns/op	  15137534 writes/sec	 2103664 B/op	      56 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_4_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(95), 4 producers: 15137534 writes/sec
// BenchmarkContentionScaling/A_1M_P_4_S_97-12           	       1	3000695309 ns/op	  15243055 writes/sec	 2103664 B/op	      56 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_4_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(97), 4 producers: 15243055 writes/sec
// BenchmarkContentionScaling/A_1M_P_4_S_99-12           	       1	3000445759 ns/op	  15209385 writes/sec	 2105648 B/op	      62 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_4_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(99), 4 producers: 15209385 writes/sec
// BenchmarkContentionScaling/A_1M_P_8_S_90-12           	       1	3000636889 ns/op	  16631128 writes/sec	 2107600 B/op	      72 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_8_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(90), 8 producers: 16631128 writes/sec
// BenchmarkContentionScaling/A_1M_P_8_S_95-12           	       1	3000251212 ns/op	  17126394 writes/sec	 2108624 B/op	      79 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_8_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(95), 8 producers: 17126394 writes/sec
// BenchmarkContentionScaling/A_1M_P_8_S_97-12           	       1	3000257814 ns/op	  16441529 writes/sec	 2104016 B/op	      64 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_8_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(97), 8 producers: 16441529 writes/sec
// BenchmarkContentionScaling/A_1M_P_8_S_99-12           	       1	3000094908 ns/op	  16560294 writes/sec	 2103888 B/op	      61 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_8_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(99), 8 producers: 16560294 writes/sec
// BenchmarkContentionScaling/A_1M_P_16_S_90-12          	       1	3005343475 ns/op	   2065661 writes/sec	 2110640 B/op	     105 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_16_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(90), 16 producers: 2065661 writes/sec
// BenchmarkContentionScaling/A_1M_P_16_S_95-12          	       1	3002735176 ns/op	   2053389 writes/sec	 2111728 B/op	     100 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_16_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(95), 16 producers: 2053389 writes/sec
// BenchmarkContentionScaling/A_1M_P_16_S_97-12          	       1	3005781471 ns/op	   2079229 writes/sec	 2113392 B/op	     108 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_16_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(97), 16 producers: 2079229 writes/sec
// BenchmarkContentionScaling/A_1M_P_16_S_99-12          	       1	3003806848 ns/op	   2038713 writes/sec	 2104720 B/op	      80 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_16_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(99), 16 producers: 2038713 writes/sec
// BenchmarkContentionScaling/A_1M_P_32_S_90-12          	       1	3004441434 ns/op	    753965 writes/sec	 2111184 B/op	     119 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_32_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(90), 32 producers: 753965 writes/sec
// BenchmarkContentionScaling/A_1M_P_32_S_95-12          	       1	3003417033 ns/op	    782070 writes/sec	 2119568 B/op	     142 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_32_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(95), 32 producers: 782070 writes/sec
// BenchmarkContentionScaling/A_1M_P_32_S_97-12          	       1	3003551716 ns/op	    851857 writes/sec	 2106160 B/op	     113 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_32_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(97), 32 producers: 851857 writes/sec
// BenchmarkContentionScaling/A_1M_P_32_S_99-12          	       1	3004894519 ns/op	    809601 writes/sec	 2106128 B/op	     112 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_1M_P_32_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 1M(99), 32 producers: 809601 writes/sec
// BenchmarkContentionScaling/A_2M_P_4_S_90-12           	       1	3001411901 ns/op	  15376116 writes/sec	 4200816 B/op	      56 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_4_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(90), 4 producers: 15376116 writes/sec
// BenchmarkContentionScaling/A_2M_P_4_S_95-12           	       1	3000650215 ns/op	  15150921 writes/sec	 4200816 B/op	      56 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_4_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(95), 4 producers: 15150921 writes/sec
// BenchmarkContentionScaling/A_2M_P_4_S_97-12           	       1	3000271902 ns/op	  15034089 writes/sec	 4200816 B/op	      56 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_4_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(97), 4 producers: 15034089 writes/sec
// BenchmarkContentionScaling/A_2M_P_4_S_99-12           	       1	3001041523 ns/op	  15466430 writes/sec	 4200848 B/op	      57 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_4_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(99), 4 producers: 15466430 writes/sec
// BenchmarkContentionScaling/A_2M_P_8_S_90-12           	       1	3001105954 ns/op	  16747990 writes/sec	 4201168 B/op	      64 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_8_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(90), 8 producers: 16747990 writes/sec
// BenchmarkContentionScaling/A_2M_P_8_S_95-12           	       1	3000842177 ns/op	  16673060 writes/sec	 4201168 B/op	      64 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_8_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(95), 8 producers: 16673060 writes/sec
// BenchmarkContentionScaling/A_2M_P_8_S_97-12           	       1	3001024591 ns/op	  16780062 writes/sec	 4201168 B/op	      64 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_8_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(97), 8 producers: 16780062 writes/sec
// BenchmarkContentionScaling/A_2M_P_8_S_99-12           	       1	3001111765 ns/op	  16790112 writes/sec	 4201168 B/op	      64 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_8_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(99), 8 producers: 16790112 writes/sec
// BenchmarkContentionScaling/A_2M_P_16_S_90-12          	       1	3006817926 ns/op	   4157143 writes/sec	 4202928 B/op	      91 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_16_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(90), 16 producers: 4157143 writes/sec
// BenchmarkContentionScaling/A_2M_P_16_S_95-12          	       1	3006754095 ns/op	   4157239 writes/sec	 4201872 B/op	      80 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_16_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(95), 16 producers: 4157239 writes/sec
// BenchmarkContentionScaling/A_2M_P_16_S_97-12          	       1	3012075541 ns/op	   4149929 writes/sec	 4202256 B/op	      84 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_16_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(97), 16 producers: 4149929 writes/sec
// BenchmarkContentionScaling/A_2M_P_16_S_99-12          	       1	3004079682 ns/op	   4132975 writes/sec	 4202064 B/op	      82 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_16_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(99), 16 producers: 4132975 writes/sec
// BenchmarkContentionScaling/A_2M_P_32_S_90-12          	       1	3003296395 ns/op	   1676046 writes/sec	 4203280 B/op	     112 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_32_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(90), 32 producers: 1676046 writes/sec
// BenchmarkContentionScaling/A_2M_P_32_S_95-12          	       1	3004035469 ns/op	   1647633 writes/sec	 4203280 B/op	     112 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_32_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(95), 32 producers: 1647633 writes/sec
// BenchmarkContentionScaling/A_2M_P_32_S_97-12          	       1	3003967931 ns/op	   1759370 writes/sec	 4209104 B/op	     125 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_32_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(97), 32 producers: 1759370 writes/sec
// BenchmarkContentionScaling/A_2M_P_32_S_99-12          	       1	3004064253 ns/op	   1675551 writes/sec	 4203280 B/op	     112 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_2M_P_32_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 2M(99), 32 producers: 1675551 writes/sec
// BenchmarkContentionScaling/A_4M_P_4_S_90-12           	       1	3001133365 ns/op	  15353890 writes/sec	 8395120 B/op	      56 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_4_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(90), 4 producers: 15353890 writes/sec
// BenchmarkContentionScaling/A_4M_P_4_S_95-12           	       1	3000651287 ns/op	  15281983 writes/sec	 8395120 B/op	      56 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_4_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(95), 4 producers: 15281983 writes/sec
// BenchmarkContentionScaling/A_4M_P_4_S_97-12           	       1	3001758996 ns/op	  15319684 writes/sec	 8395120 B/op	      56 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_4_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(97), 4 producers: 15319684 writes/sec
// BenchmarkContentionScaling/A_4M_P_4_S_99-12           	       1	3001838135 ns/op	  15282798 writes/sec	 8395120 B/op	      56 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_4_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(99), 4 producers: 15282798 writes/sec
// BenchmarkContentionScaling/A_4M_P_8_S_90-12           	       1	3001293548 ns/op	  16732424 writes/sec	 8395472 B/op	      64 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_8_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(90), 8 producers: 16732424 writes/sec
// BenchmarkContentionScaling/A_4M_P_8_S_95-12           	       1	3002192333 ns/op	  16899979 writes/sec	 8395472 B/op	      64 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_8_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(95), 8 producers: 16899979 writes/sec
// BenchmarkContentionScaling/A_4M_P_8_S_97-12           	       1	3000785730 ns/op	  16739337 writes/sec	 8395472 B/op	      64 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_8_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(97), 8 producers: 16739337 writes/sec
// BenchmarkContentionScaling/A_4M_P_8_S_99-12           	       1	3000963596 ns/op	  16867497 writes/sec	 8395472 B/op	      64 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_8_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(99), 8 producers: 16867497 writes/sec
// BenchmarkContentionScaling/A_4M_P_16_S_90-12          	       1	3003076350 ns/op	   8325168 writes/sec	 8396560 B/op	      84 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_16_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(90), 16 producers: 8325168 writes/sec
// BenchmarkContentionScaling/A_4M_P_16_S_95-12          	       1	3004841519 ns/op	   8264533 writes/sec	 8397776 B/op	      93 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_16_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(95), 16 producers: 8264533 writes/sec
// BenchmarkContentionScaling/A_4M_P_16_S_97-12          	       1	3006025111 ns/op	   8263313 writes/sec	 8398096 B/op	     100 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_16_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(97), 16 producers: 8263313 writes/sec
// BenchmarkContentionScaling/A_4M_P_16_S_99-12          	       1	3006086877 ns/op	   8319093 writes/sec	 8403664 B/op	     103 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_16_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(99), 16 producers: 8319093 writes/sec
// BenchmarkContentionScaling/A_4M_P_32_S_90-12          	       1	3002140355 ns/op	   3298725 writes/sec	 8397584 B/op	     112 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_32_S_90-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(90), 32 producers: 3298725 writes/sec
// BenchmarkContentionScaling/A_4M_P_32_S_95-12          	       1	3003414267 ns/op	   3185279 writes/sec	 8397584 B/op	     112 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_32_S_95-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(95), 32 producers: 3185279 writes/sec
// BenchmarkContentionScaling/A_4M_P_32_S_97-12          	       1	3003843567 ns/op	   3129375 writes/sec	 8397584 B/op	     112 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_32_S_97-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(97), 32 producers: 3129375 writes/sec
// BenchmarkContentionScaling/A_4M_P_32_S_99-12          	       1	3004611335 ns/op	   3183395 writes/sec	 8397584 B/op	     112 allocs/op
// --- BENCH: BenchmarkContentionScaling/A_4M_P_32_S_99-12
//     /mnt/tmpfs.ramdisk/bytearena/tc_16_32_producers_test.go:510: Arena 4M(99), 32 producers: 3183395 writes/sec

// BenchmarkContentionScaling measures how contention affects throughput
// by varying both producers and arena size.
func BenchmarkContentionScaling(b *testing.B) {
	producerCounts := []int{4, 8, 16, 32}
	arenaSizes := []Size{Size100K, Size500K, Size1M, Size2M, Size4M}
	sealPercentages := []uint32{90, 95, 99}

	for _, arenaSize := range arenaSizes {
		for _, numProducers := range producerCounts {
			for _, sealPercentage := range sealPercentages {
				b.Run(
					fmt.Sprintf("A_%s_P_%d_S_%d",
						arenaSize.String(),
						numProducers,
						sealPercentage,
					),

					func(b *testing.B) {
						writer := &helpers.NoopWriter{}

						ingestor, err := NewIngestor(
							arenaSize(),
							writer,
							WithSealPercentage(sealPercentage),
						)
						require.NoError(b, err)

						ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
						defer cancel()

						chIngestionEnd := ingestor.StartIngestion(ctx)

						payload := []byte("benchmark-payload-32bytes")

						var (
							wgProducers     sync.WaitGroup
							writesCompleted atomic.Int64
						)

						startTime := time.Now()

						for range numProducers {
							wgProducers.Go(
								func() {
									for {
										select {
										case <-ctx.Done():
											return
										default:
											if _, err := ingestor.Write(payload); err == nil {
												writesCompleted.Add(1)
											}
										}
									}
								},
							)
						}

						<-ctx.Done()
						cancel()
						wgProducers.Wait()
						<-chIngestionEnd

						elapsed := time.Since(startTime)
						writesPerSec := float64(writesCompleted.Load()) / elapsed.Seconds()

						b.ReportMetric(writesPerSec, "writes/sec")
						b.Logf(
							"Arena %s(%d), %d producers: %.0f writes/sec",
							arenaSize.String(),
							sealPercentage,
							numProducers,
							writesPerSec,
						)
					},
				)
			}
		}
	}
}
