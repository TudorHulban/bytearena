package helpers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGoidOffset(t *testing.T) {
	t.Parallel()

	id1 := GoroutineID()
	require.NotZero(t, id1)

	ch := make(chan int64, 1)

	go func() {
		ch <- GoroutineID()
	}()

	id2 := <-ch
	require.NotZero(t, id2)

	// 1. Error case: IDs must differ
	require.NotEqual(t, id1, id2, "goroutine IDs must differ")

	// 2. Success case: print for manual inspection
	fmt.Println(id1, id2)
}

// BenchmarkGoid-16    	732586599	         1.640 ns/op	       0 B/op	       0 allocs/op
func BenchmarkGoid(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		id := GoroutineID()
		if id == 0 {
			b.Fatal("invalid goid")
		}
	}
}
