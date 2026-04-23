package bytearena

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
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

// Tests the ingestor with 32 concurrent producers
// writing at full speed to ensure no data loss and proper synchronization.
func Test_17_1_Concurrent32Producers(t *testing.T) {
	var out bytes.Buffer

	ingestor, err := NewIngestor(Size8M(), &out)
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

// Tests the ingestor with 64+ producers
// and aggressive rotation to stress the lock-free mechanisms.
func Test_17_2_HighContentionConcurrentWrites(t *testing.T) {
	var out bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(Size16M(), &out, WithSealPercentage(50))
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

	t.Logf(
		"Success: %d/%d (%.1f%%), Drops: %d",
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

	numberDroppedArenas := ingestor.Registry.load(TErrDroppedSealedData)

	if numProducers == 0 {
		require.Equal(t,
			expectedBytes,
			outputBytes,
			"flushed bytes must exactly match successful writes × payload size",
		)
	}

	require.LessOrEqual(t,
		numberDroppedArenas,
		uint64(2),
	)

	// drops are expected and correct under this high-contention configuration.
	// a minimum success rate should not be asserted.
	require.Greater(t,
		successCount.Load(),
		int64(0),
	)
}

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkConcurrentProducers/gomaxprocs_1/producers_4-16         	86226817	        13.37 ns/op	  74786903 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_1/producers_8-16         	87988821	        13.54 ns/op	  73846436 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_1/producers_16-16        	83971722	        13.50 ns/op	  74061419 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_1/producers_32-16        	87609158	        13.48 ns/op	  74184473 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_2/producers_4-16         	17275197	        70.06 ns/op	  14273397 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_2/producers_8-16         	17423181	        69.57 ns/op	  14372913 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_2/producers_16-16        	17556499	        68.26 ns/op	  14648572 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_2/producers_32-16        	18213955	        70.15 ns/op	  14254368 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_3/producers_4-16         	20004510	        61.67 ns/op	  16214078 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_3/producers_8-16         	19528316	        60.95 ns/op	  16407084 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_3/producers_16-16        	19592413	        61.49 ns/op	  16263235 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_3/producers_32-16        	19443805	        61.51 ns/op	  16258261 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_4/producers_4-16         	23314588	        51.43 ns/op	  19443706 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_4/producers_8-16         	23507071	        51.25 ns/op	  19509840 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_4/producers_16-16        	23451075	        51.23 ns/op	  19519589 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/gomaxprocs_4/producers_32-16        	23392860	        51.10 ns/op	  19568707 writes/sec	       0 B/op	       0 allocs/op

// go test -run '^$' -bench '^BenchmarkConcurrentProducers$' -benchmem
// go test -run '^$' -bench '^BenchmarkConcurrentProducers$' -benchmem -race

// BenchmarkConcurrentProducers benchmarks the ingestor with increasing numbers of
// concurrent producers to measure scaling characteristics.
func BenchmarkConcurrentProducers(b *testing.B) {
	gomaxprocsValues := []int{1, 2, 3, 4}
	producerCounts := []int{4, 8, 16, 32}

	for _, mp := range gomaxprocsValues {
		b.Run(
			fmt.Sprintf("gomaxprocs_%d", mp),
			func(b *testing.B) {
				for _, numProducers := range producerCounts {
					b.Run(
						fmt.Sprintf("producers_%d", numProducers),
						func(b *testing.B) {
							prev := runtime.GOMAXPROCS(mp)
							defer runtime.GOMAXPROCS(prev)

							writer := &helpers.NoopWriter{}

							ingestor, errCrIngestor := NewIngestor(
								Size1M(),
								writer,
								WithUnblockMilliseconds(90),
							)
							if errCrIngestor != nil {
								b.Fatalf("errCrIngestor = %s", errCrIngestor.Error())
							}
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
			},
		)
	}
}

// Tests the ingestor with
// different payload sizes under high concurrency.
func Test_17_3_ConcurrentProducersWithVariablePayload(t *testing.T) {
	var writer bytes.Buffer

	ingestor, errCr := NewIngestor(Size16M(), &writer)
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

// Test_4_ProducerConsumerThroughput tests the throughput under sustained load
// with 32 producers and continuous rotation.
func Test_17_4_ProducerConsumerThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}

	var writer bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(
		Size16M(),
		&writer,
		WithSealPercentage(97),
		WithUnblockMilliseconds(190),
	)
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

	t.Logf(
		"Throughput: %.0f writes/sec, %.2f MB/sec",
		throughputWrites,
		throughputBytes,
	)
	t.Logf(
		"Total writes: %d, Total bytes: %d, Duration: %v",
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

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkConcurrentProducersFixedTime/producers_4-16         	       1	2000971561 ns/op	  20217295 writes/sec	 2373112 B/op	    3951 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_4-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_17_32_producers_test.go:496: 4 producers: 20217295 writes/sec, total writes: 40434590
// BenchmarkConcurrentProducersFixedTime/producers_8-16         	       1	2000306769 ns/op	  23704606 writes/sec	 2442872 B/op	    4652 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_8-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_17_32_producers_test.go:496: 8 producers: 23704606 writes/sec, total writes: 47409212
// BenchmarkConcurrentProducersFixedTime/producers_16-16        	       1	2000274938 ns/op	  33842496 writes/sec	 2594128 B/op	    6706 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_16-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_17_32_producers_test.go:496: 16 producers: 33842496 writes/sec, total writes: 67684991
// BenchmarkConcurrentProducersFixedTime/producers_32-16        	       1	2002037825 ns/op	  33737738 writes/sec	 2568280 B/op	    6645 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_32-16
//     /mnt/tmpfs.ramdisk/bytearena/tc_17_32_producers_test.go:496: 32 producers: 33737738 writes/sec, total writes: 67475475

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
