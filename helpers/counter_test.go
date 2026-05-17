package helpers

import (
	"fmt"
	"sync/atomic"
	"testing"
)

func TestCPUCounterBouncingDX(t *testing.T) {
	counter := NewCPUCounter()

	// Call Next() back-to-back on the same thread
	val1 := counter.Next()
	val2 := counter.Next()
	val3 := counter.Next()

	fmt.Println("First read: ", val1) // Outputs: 1
	fmt.Println("Second read:", val2) // Outputs: 2
	fmt.Println("Third read: ", val3) // Outputs: 3

	if val2 <= val1 || val3 <= val2 {
		t.Errorf("The counter did not bounce! Got sequence: %d, %d, %d", val1, val2, val3)
	}
}

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkStandardAtomic_Serial-16    	653816366	         1.844 ns/op	       0 B/op	       0 allocs/op

func BenchmarkStandardAtomic_Serial(b *testing.B) {
	var globalCounter uint64

	b.ResetTimer()

	for b.Loop() {
		atomic.AddUint64(&globalCounter, 1)
	}
}

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkStandardAtomic-16    	92128544	        13.72 ns/op	       0 B/op	       0 allocs/op

// BenchmarkStandardAtomic measures a standard global atomic counter.
// On high-core machines, this will suffer heavily from cache-line bouncing.
func BenchmarkStandardAtomic(b *testing.B) {
	var globalCounter uint64

	b.ResetTimer()
	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				atomic.AddUint64(&globalCounter, 1)
			}
		},
	)
}

// BenchmarkCPUCounter_Serial-16    	229903899	         5.218 ns/op	       0 B/op	       0 allocs/op
func BenchmarkCPUCounter_Serial(b *testing.B) {
	counter := NewCPUCounter()

	b.ResetTimer()

	for b.Loop() {
		counter.Next()
	}
}

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkCPUCounter_Parallel-16    	1000000000	         0.5137 ns/op	       0 B/op	       0 allocs/op

// BenchmarkCPUCounter measures the sharded counter.
// Performance should stay flat and extremely fast regardless of core count.
func BenchmarkCPUCounter_Parallel(b *testing.B) {
	counter := NewCPUCounter()

	b.ResetTimer()
	b.RunParallel(
		func(pb *testing.PB) {
			for pb.Next() {
				counter.Next()
			}
		},
	)
}
