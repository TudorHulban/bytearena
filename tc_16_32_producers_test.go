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

// Test Case 16: Benchmark under 32+ goroutines.
// This test verifies the ingestor's performance and correctness under high concurrency
// with multiple producers writing simultaneously while the consumer rotates arenas.

// TestConcurrent32Producers tests the ingestor with 32 concurrent producers
// writing at full speed to ensure no data loss and proper synchronization.
func TestConcurrent32Producers(t *testing.T) {
	var out bytes.Buffer

	ingestor, err := NewIngestor(Size1M, &out)
	require.NoError(t, err)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wgConsumer sync.WaitGroup
	wgConsumer.Add(1)

	go func() {
		defer wgConsumer.Done()
		ingestor.consumerLoop(ctx)
	}()

	numProducers := 32
	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	successCount := atomic.Int64{}
	writesPerProducer := 5000

	for i := range numProducers {
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
		}(i)
	}

	wgProducers.Wait()
	cancel()
	wgConsumer.Wait()

	output := out.String()
	require.NotEmpty(t, output, "expected non-empty output")

	// Count lines in output
	outputLines := bytes.Count(out.Bytes(), []byte("\n"))

	t.Logf("Successful writes: %d, Output lines: %d", successCount.Load(), outputLines)

	// We might have some failures due to backpressure, but success count should be reasonable
	require.Greater(t, successCount.Load(), int64(0), "should have at least some successful writes")
	require.LessOrEqual(t, outputLines, int(successCount.Load()), "output lines should not exceed successful writes")
}

// TestHighContentionConcurrentWrites tests the ingestor with 64+ producers
// and aggressive rotation to stress the lock-free mechanisms.
func TestHighContentionConcurrentWrites(t *testing.T) {
	var out bytes.Buffer

	// Use smaller arena to increase rotation frequency and contention
	ingestor, err := NewIngestor(Size100K, &out, WithSealPercentage(50))
	require.NoError(t, err)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wgConsumer sync.WaitGroup
	wgConsumer.Add(1)

	go func() {
		defer wgConsumer.Done()
		ingestor.consumerLoop(ctx)
	}()

	numProducers := 64
	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	successCount := atomic.Int64{}
	writesPerProducer := 10000

	// Use a wait group to ensure all producers start roughly simultaneously
	var startWG sync.WaitGroup
	startWG.Add(1)

	for i := range numProducers {
		go func(producerID int) {
			startWG.Wait() // Wait for all producers to be ready

			defer wgProducers.Done()

			for j := range writesPerProducer {
				payload := fmt.Sprintf("p%d-w%d\n", producerID, j)

				if err := ingestor.write(
					uint32(len(payload)),
					func(dst []byte) {
						copy(dst, []byte(payload))
					},
				); err == nil {
					successCount.Add(1)
				}
			}
		}(i)
	}

	// Start all producers simultaneously
	startWG.Done()

	wgProducers.Wait()
	cancel()
	wgConsumer.Wait()

	totalWrites := int64(numProducers * writesPerProducer)

	t.Logf(
		"Total possible writes: %d, Successful: %d, Success rate: %.2f%%",
		totalWrites,
		successCount.Load(),
		float64(successCount.Load())/float64(totalWrites)*100,
	)

	require.Greater(t, successCount.Load(), totalWrites/2, "should have at least 50%% success rate under high contention")
}

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkConcurrentProducers/producers_4-16         	19729136	        57.92 ns/op	    550618 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/producers_8-16         	19806210	        57.28 ns/op	    305252 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/producers_16-16        	19048885	        57.50 ns/op	    353196 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/producers_32-16        	20298030	        56.89 ns/op	    151475 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/producers_64-16        	19612126	        56.75 ns/op	     84011 writes/sec	       0 B/op	       0 allocs/op
// BenchmarkConcurrentProducers/producers_128-16       	19718438	        56.93 ns/op	    132982 writes/sec	       0 B/op	       0 allocs/op

// BenchmarkConcurrentProducers benchmarks the ingestor with increasing numbers of
// concurrent producers to measure scaling characteristics.
func BenchmarkConcurrentProducers(b *testing.B) {
	producerCounts := []int{4, 8, 16, 32, 64, 128}

	for _, numProducers := range producerCounts {
		b.Run(
			fmt.Sprintf("producers_%d", numProducers),
			func(b *testing.B) {
				writer := &helpers.NoopWriter{}

				ingestor, err := NewIngestor(Size1M, writer)
				require.NoError(b, err)
				require.NotNil(b, ingestor)

				ctx, cancel := context.WithCancel(context.Background())
				chIngestionEnd := ingestor.StartIngestion(ctx)

				payload := []byte("benchmark-payload-32bytes")

				b.ResetTimer()
				b.SetParallelism(numProducers)

				var writesCompleted atomic.Int64

				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						if _, errWrite := ingestor.Write(payload); errWrite == nil {
							writesCompleted.Add(1)
						}
					}
				})

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

	ingestor, err := NewIngestor(Size1M, &out)
	require.NoError(t, err)
	require.NotNil(t, ingestor)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wgConsumer sync.WaitGroup
	wgConsumer.Add(1)

	go func() {
		defer wgConsumer.Done()
		ingestor.consumerLoop(ctx)
	}()

	numProducers := 32
	var wgProducers sync.WaitGroup
	wgProducers.Add(numProducers)

	successCount := atomic.Int64{}
	totalBytesWritten := atomic.Int64{}

	payloadSizes := []int{16, 64, 256, 1024}

	for i := range numProducers {
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
		}(i)
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
// with 32+ producers and continuous rotation.
func TestProducerConsumerThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping throughput test in short mode")
	}

	var out bytes.Buffer

	ingestor, errCrIngestor := NewIngestor(Size500K, &out, WithSealPercentage(70))
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

	numProducers := 48
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
		successCount.Load(), totalBytes.Load(), elapsed)

	require.Greater(t,
		successCount.Load(),
		int64(10000),
		"should have processed at least 10000 writes",
	)
}

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkConcurrentProducersFixedTime/producers_4-16         	       1	2000320484 ns/op	  13300533 writes/sec	2148577328 B/op	     141 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_4-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:383: 4 producers: 13300533 writes/sec, total writes: 26601066
// BenchmarkConcurrentProducersFixedTime/producers_8-16         	       1	2000654589 ns/op	  15051729 writes/sec	2148563128 B/op	     129 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_8-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:383: 8 producers: 15051729 writes/sec, total writes: 30103458
// BenchmarkConcurrentProducersFixedTime/producers_16-16        	       1	2000810554 ns/op	   2055207 writes/sec	269502560 B/op	     126 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_16-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:383: 16 producers: 2055207 writes/sec, total writes: 4110414
// BenchmarkConcurrentProducersFixedTime/producers_32-16        	       1	2018485831 ns/op	   1279262 writes/sec	135293584 B/op	     160 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_32-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:383: 32 producers: 1279262 writes/sec, total writes: 2558523
// BenchmarkConcurrentProducersFixedTime/producers_64-16        	       1	2015684072 ns/op	    524288 writes/sec	68203696 B/op	     252 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_64-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:383: 64 producers: 524288 writes/sec, total writes: 1048575
// BenchmarkConcurrentProducersFixedTime/producers_128-16       	       1	2017934144 ns/op	    251658 writes/sec	34682688 B/op	     439 allocs/op
// --- BENCH: BenchmarkConcurrentProducersFixedTime/producers_128-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:383: 128 producers: 251658 writes/sec, total writes: 503316

// BenchmarkConcurrentProducersFixedTime benchmarks with fixed duration
// to get accurate throughput measurements across different producer counts.
func BenchmarkConcurrentProducersFixedTime(b *testing.B) {
	producerCounts := []int{4, 8, 16, 32, 64, 128}
	duration := 2 * time.Second

	for _, numProducers := range producerCounts {
		b.Run(
			fmt.Sprintf("producers_%d", numProducers),
			func(b *testing.B) {
				writer := &bytes.Buffer{}

				ingestor, err := NewIngestor(Size1M, writer)
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

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkContentionScaling/arena_102400_producers_4-16         	       1	3000114792 ns/op	  12421229 writes/sec	  226368 B/op	      98 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_102400_producers_4-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 102400, 4 producers: 12421229 writes/sec
// BenchmarkContentionScaling/arena_102400_producers_8-16         	       1	3000068543 ns/op	  13568655 writes/sec	  251328 B/op	     134 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_102400_producers_8-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 102400, 8 producers: 13568655 writes/sec
// BenchmarkContentionScaling/arena_102400_producers_16-16        	       1	3003305779 ns/op	    203214 writes/sec	  261600 B/op	     223 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_102400_producers_16-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 102400, 16 producers: 203214 writes/sec
// BenchmarkContentionScaling/arena_102400_producers_32-16        	       1	3010559197 ns/op	    107526 writes/sec	  240000 B/op	     154 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_102400_producers_32-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 102400, 32 producers: 107526 writes/sec
// BenchmarkContentionScaling/arena_102400_producers_64-16        	       1	3010862042 ns/op	     48976 writes/sec	  256576 B/op	     241 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_102400_producers_64-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 102400, 64 producers: 48976 writes/sec
// BenchmarkContentionScaling/arena_102400_producers_128-16       	       1	3000304912 ns/op	     24583 writes/sec	  290176 B/op	     428 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_102400_producers_128-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 102400, 128 producers: 24583 writes/sec
// BenchmarkContentionScaling/arena_512000_producers_4-16         	       1	3001056539 ns/op	  14533595 writes/sec	 1039808 B/op	      62 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_512000_producers_4-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 512000, 4 producers: 14533595 writes/sec
// BenchmarkContentionScaling/arena_512000_producers_8-16         	       1	3000346702 ns/op	  16381340 writes/sec	 1044512 B/op	      81 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_512000_producers_8-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 512000, 8 producers: 16381340 writes/sec
// BenchmarkContentionScaling/arena_512000_producers_16-16        	       1	3007355857 ns/op	   1014731 writes/sec	 1049456 B/op	     162 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_512000_producers_16-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 512000, 16 producers: 1014731 writes/sec
// BenchmarkContentionScaling/arena_512000_producers_32-16        	       1	3007442521 ns/op	    599276 writes/sec	 1061600 B/op	     148 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_512000_producers_32-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 512000, 32 producers: 599276 writes/sec
// BenchmarkContentionScaling/arena_512000_producers_64-16        	       1	3005853605 ns/op	    245293 writes/sec	 1075056 B/op	     241 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_512000_producers_64-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 512000, 64 producers: 245293 writes/sec
// BenchmarkContentionScaling/arena_512000_producers_128-16       	       1	3004266162 ns/op	    102344 writes/sec	 1100144 B/op	     408 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_512000_producers_128-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 512000, 128 producers: 102344 writes/sec
// BenchmarkContentionScaling/arena_1048576_producers_4-16        	       1	3000801836 ns/op	  14736966 writes/sec	 2104608 B/op	      59 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_1048576_producers_4-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 1048576, 4 producers: 14736966 writes/sec
// BenchmarkContentionScaling/arena_1048576_producers_8-16        	       1	3000228658 ns/op	  16727010 writes/sec	 2106432 B/op	      67 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_1048576_producers_8-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 1048576, 8 producers: 16727010 writes/sec
// BenchmarkContentionScaling/arena_1048576_producers_16-16       	       1	3010195768 ns/op	   2062256 writes/sec	 2111968 B/op	     140 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_1048576_producers_16-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 1048576, 16 producers: 2062256 writes/sec
// BenchmarkContentionScaling/arena_1048576_producers_32-16       	       1	3011353465 ns/op	   1156107 writes/sec	 2116672 B/op	     135 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_1048576_producers_32-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 1048576, 32 producers: 1156107 writes/sec
// BenchmarkContentionScaling/arena_1048576_producers_64-16       	       1	3008574500 ns/op	    460072 writes/sec	 2140752 B/op	     241 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_1048576_producers_64-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 1048576, 64 producers: 460072 writes/sec
// BenchmarkContentionScaling/arena_1048576_producers_128-16      	       1	3004602480 ns/op	    279202 writes/sec	 2178576 B/op	     420 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_1048576_producers_128-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 1048576, 128 producers: 279202 writes/sec
// BenchmarkContentionScaling/arena_2097152_producers_4-16        	       1	3001076427 ns/op	  14834113 writes/sec	 4201648 B/op	      58 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_2097152_producers_4-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 2097152, 4 producers: 14834113 writes/sec
// BenchmarkContentionScaling/arena_2097152_producers_8-16        	       1	3000669314 ns/op	  16862778 writes/sec	 4201664 B/op	      63 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_2097152_producers_8-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 2097152, 8 producers: 16862778 writes/sec
// BenchmarkContentionScaling/arena_2097152_producers_16-16       	       1	3013953260 ns/op	   4147448 writes/sec	 4211216 B/op	     158 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_2097152_producers_16-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 2097152, 16 producers: 4147448 writes/sec
// BenchmarkContentionScaling/arena_2097152_producers_32-16       	       1	3004219924 ns/op	   2429441 writes/sec	 4203808 B/op	     112 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_2097152_producers_32-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 2097152, 32 producers: 2429441 writes/sec
// BenchmarkContentionScaling/arena_2097152_producers_64-16       	       1	3004788583 ns/op	   1005093 writes/sec	 4206592 B/op	     175 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_2097152_producers_64-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 2097152, 64 producers: 1005093 writes/sec
// BenchmarkContentionScaling/arena_2097152_producers_128-16      	       1	3005292450 ns/op	    474551 writes/sec	 4250624 B/op	     383 allocs/op
// --- BENCH: BenchmarkContentionScaling/arena_2097152_producers_128-16
//     /home/tudi/ram/bytearena/tc_16_32_producers_test.go:497: Arena 2097152, 128 producers: 474551 writes/sec

// BenchmarkContentionScaling measures how contention affects throughput
// by varying both producers and arena size.
func BenchmarkContentionScaling(b *testing.B) {
	producerCounts := []int{4, 8, 16, 32, 64, 128}
	arenaSizes := []int{Size100K, Size500K, Size1M, Size2M}

	for _, arenaSize := range arenaSizes {
		for _, numProducers := range producerCounts {
			b.Run(
				fmt.Sprintf("arena_%d_producers_%d", arenaSize, numProducers),
				func(b *testing.B) {
					writer := &helpers.NoopWriter{}

					ingestor, err := NewIngestor(uint32(arenaSize), writer)
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
						"Arena %d, %d producers: %.0f writes/sec",
						arenaSize,
						numProducers,
						writesPerSec,
					)
				},
			)
		}
	}
}
