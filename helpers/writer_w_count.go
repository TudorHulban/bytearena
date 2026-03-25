package helpers

import "sync/atomic"

// real writer that has observable side effects
// thus the compiler cannot eliminate code.
type CountWriter struct {
	TotalBytesWritten atomic.Int64
	NumberWrites      atomic.Int64
}

func (w *CountWriter) Write(p []byte) (int, error) {
	w.TotalBytesWritten.Add(int64(len(p)))
	w.NumberWrites.Add(1)

	return len(p), nil
}
