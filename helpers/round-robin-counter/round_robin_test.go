//go:build amd64 && linux

package roundrobincounter_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	roundrobincounter "github.com/tudorhulban/bytearena/helpers/round-robin-counter"
)

func TestRoundRobinCounterSequential(t *testing.T) {
	counter := roundrobincounter.NewRoundRobinCounter()

	// Expect strictly increasing values modulo 2^64.
	var last uint64

	for ix := range 1_000_000 {
		value := counter.Next()

		if ix > 0 && value != last+1 {
			t.Fatalf(
				"expected %d, got %d",
				last+1,
				value,
			)
		}

		last = value
	}
}

func TestRoundRobinCounterConcurrent(t *testing.T) {
	counter := roundrobincounter.NewRoundRobinCounter()

	const noRequests = 1_000_000

	results := make(chan uint64, 2*noRequests)

	worker := func() {
		for range noRequests {
			results <- counter.Next()
		}
	}

	go worker()
	go worker()

	seen := make(map[uint64]int)

	for range 2 * noRequests {
		v := <-results
		seen[v]++
	}

	// Count duplicates: values returned more than once.
	dups := 0

	for _, c := range seen {
		if c > 1 {
			dups = dups + c - 1
		}
	}

	t.Logf("total increments: %d", 2*noRequests)
	t.Logf("unique values:    %d", len(seen))
	t.Logf("duplicates:        %d", dups)
}

func TestRoundRobinCounter_Heavy(t *testing.T) {
	counter := roundrobincounter.NewRoundRobinCounter()

	const noRequests = 5_000_000

	results := make([]uint64, 2*noRequests)

	var wg sync.WaitGroup
	wg.Add(2)

	// Pin both goroutines to the same P/core.
	runtime.GOMAXPROCS(1)

	go func() {
		defer wg.Done()

		for i := range noRequests {
			results[i] = counter.Next()
		}
	}()

	go func() {
		defer wg.Done()

		for i := range noRequests {
			// tiny desync to increase collision probability
			_ = i * 17

			results[noRequests+i] = counter.Next()
		}
	}()

	wg.Wait()

	seen := make(map[uint64]int, 2*noRequests)
	dups := 0

	for _, v := range results {
		seen[v]++
	}

	for _, c := range seen {
		if c > 1 {
			dups = dups + c - 1
		}
	}

	fmt.Printf("total increments: %d.\n", 2*noRequests)
	fmt.Printf("unique values:    %d.\n", len(seen))
	fmt.Printf("duplicates:       %d.\n", dups)
}

// ── benchmarks ────────────────────────────────────────────────────────────────

// BenchmarkAtomicAdd is the baseline: what sync/atomic costs.
// BenchmarkAtomicAdd-16    	93469831	        13.66 ns/op	       0 B/op	       0 allocs/op
func BenchmarkAtomicAdd(b *testing.B) {
	var c atomic.Uint64

	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				c.Add(1)
			}
		},
	)
}

// BenchmarkFastInc-16    	125569638	         9.602 ns/op	       0 B/op	       0 allocs/op
// BenchmarkFastInc: no LOCK prefix.
func BenchmarkFastInc(b *testing.B) {
	counter := roundrobincounter.NewRoundRobinCounter()

	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				counter.Next()
			}
		},
	)
}

// BenchmarkFastInc_serial: single-goroutine baseline — shows raw instruction cost
// without cache-line ping-pong between cores.

// BenchmarkFastInc_serial-16    	858570014	         1.389 ns/op	       0 B/op	       0 allocs/op
func BenchmarkRobinCounter_serial(b *testing.B) {
	counter := roundrobincounter.NewRoundRobinCounter()

	for b.Loop() {
		counter.Next()
	}
}

// BenchmarkAtomicAdd_serial-16    	664979708	         1.817 ns/op	       0 B/op	       0 allocs/op
func BenchmarkAtomicAdd_serial(b *testing.B) {
	var c int64

	for b.Loop() {
		atomic.AddInt64(&c, 1)
	}
}
