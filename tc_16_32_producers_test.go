package bytearena

// Test Case 16: Benchmark under 32+ goroutines.
// tc_16_32_producers_test.go

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

// Test Case 16: Benchmark under 32 producers.
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
	ingestor, err := NewIngestor(Size100K(), &out, WithSealPercentage(50))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	chIngestionEnd := ingestor.StartIngestion(ctx)

	numProducers := 64
	writesPerProducer := 10000
	payload := []byte("p-write\n") // fixed size, no allocation

	var wgProducers sync.WaitGroup
	var successCount, dropCount atomic.Int64

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

				var wgProducers sync.WaitGroup
				var writesCompleted atomic.Int64

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

// BenchmarkContentionScaling measures how contention affects throughput
// by varying both producers and arena size.
func BenchmarkContentionScaling(b *testing.B) {
	producerCounts := []int{4, 8, 16, 32}
	arenaSizes := []Size{Size100K, Size500K, Size1M, Size2M, Size4M}
	sealPercentages := []uint32{90, 95, 97, 99}

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

						var wgProducers sync.WaitGroup
						var writesCompleted atomic.Int64

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
							"Arena %s, %d producers: %.0f writes/sec",
							arenaSize.String(),
							numProducers,
							writesPerSec,
						)
					},
				)
			}
		}
	}
}
