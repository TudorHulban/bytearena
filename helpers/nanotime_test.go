package helpers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNanotimeFunctional(t *testing.T) {
	t.Run(
		"1_monotonic_increasing",
		func(t *testing.T) {
			prev := Nanotime()

			for range 1000 {
				now := Nanotime()
				require.GreaterOrEqual(t, now, prev)
				prev = now
			}
		},
	)

	/*
	   nanotime() is a monotonic clock.
	   It does NOT share the same epoch as time.Now().UnixNano(),
	   so absolute values cannot be compared.

	   What IS true:
	       Δ(nanotime) ≈ Δ(time.Now().UnixNano())

	   Meaning:
	       nanotime increases at the same RATE as real time,
	       even though the absolute numbers are unrelated.

	   This test verifies that property.
	*/
	t.Run(
		"2_rate_matches_wall_clock",
		func(t *testing.T) {
			startNT := Nanotime()
			startWall := time.Now()

			time.Sleep(10 * time.Millisecond)

			endNT := Nanotime()
			endWall := time.Now()

			deltaNT := endNT - startNT
			deltaWall := endWall.Sub(startWall).Nanoseconds()

			// Allow 5% drift due to scheduler jitter.
			require.InDelta(t,
				float64(deltaWall),
				float64(deltaNT),
				float64(deltaWall)*0.05,

				"Δnanotime=%d ns Δwall=%d ns",
				deltaNT,
				deltaWall,
			)
		},
	)
}

// BenchmarkNanotime-16    	54018580	        21.70 ns/op	       0 B/op	       0 allocs/op
func BenchmarkNanotime(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	var sink int64

	for b.Loop() {
		sink = Nanotime()
	}

	require.NotZero(b, sink)
}

// BenchmarkNow-16    	28516358	        41.63 ns/op	       0 B/op	       0 allocs/op
func BenchmarkTimeNow(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	var sink int64

	for b.Loop() {
		sink = time.Now().UnixNano()
	}

	require.NotZero(b, sink)
}
