package helpers

import (
	"bytes"
	"sync/atomic"
)

// real writer that has observable side effects
// thus the compiler cannot eliminate code.
type CountWriterNoBuffer struct {
	TotalBytesWritten atomic.Int64
	NumberWrites      atomic.Int64
}

func (w *CountWriterNoBuffer) Write(payload []byte) (int, error) {
	w.TotalBytesWritten.Add(int64(len(payload)))
	w.NumberWrites.Add(1)

	return len(payload), nil
}

// real writer that has observable side effects
// thus the compiler cannot eliminate code.
type CountWriterWithBuffer struct {
	Buf bytes.Buffer

	TotalBytesWritten atomic.Int64
	NumberWrites      atomic.Int64
}

func (w *CountWriterWithBuffer) Write(payload []byte) (int, error) {
	w.TotalBytesWritten.Add(int64(len(payload)))
	w.NumberWrites.Add(1)

	return w.Buf.Write(payload)
}
