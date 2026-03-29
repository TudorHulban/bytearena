package helpers

import "testing"

func TestPause(t *testing.T) {
	Pause(1)
}

// BenchmarkPause_01    	67847871	        17.30 ns/op	       0 B/op	       0 allocs/op
func BenchmarkPause_01(b *testing.B) {
	for b.Loop() {
		Pause(1)
	}
}

// BenchmarkPause_03-12    	24529618	        48.56 ns/op	       0 B/op	       0 allocs/op
func BenchmarkPause_03(b *testing.B) {
	for b.Loop() {
		Pause(3)
	}
}
