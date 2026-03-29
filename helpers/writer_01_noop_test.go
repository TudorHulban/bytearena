package helpers

import "testing"

func BenchmarkNoopWriter(b *testing.B) {
	writer := NoopWriter{}
	buf := make([]byte, 128)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		writer.Write(buf)
	}
}
