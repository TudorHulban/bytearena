package helpers

import (
	"bytes"
	"sync/atomic"
)

// real writer that has observable side effects
// thus the compiler cannot eliminate code.
type CountWriter struct {
	Buf bytes.Buffer

	TotalBytesWritten atomic.Int64
	NumberWrites      atomic.Int64
}

func (w *CountWriter) Write(payload []byte) (int, error) {
	w.TotalBytesWritten.Add(int64(len(payload)))
	w.NumberWrites.Add(1)

	return w.Buf.Write(payload)
}
