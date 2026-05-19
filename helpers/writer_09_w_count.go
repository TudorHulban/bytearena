package helpers

import (
	"bytes"
	"sync/atomic"
	"unsafe"
)

// real writer that has observable side effects
// thus the compiler cannot eliminate code.
type CountWriterNoBuffer struct {
	TotalBytesWritten atomic.Uint64
	_                 [64 - unsafe.Sizeof(atomic.Uint64{})]byte
}

func (w *CountWriterNoBuffer) Write(payload []byte) (int, error) {
	w.TotalBytesWritten.Add(uint64(len(payload)))

	return len(payload), nil
}

func (w *CountWriterNoBuffer) Reset() {
	w.TotalBytesWritten.Store(0)
}

// real writer that has observable side effects
// thus the compiler cannot eliminate code.
type CountWriterWithBuffer struct {
	Buf bytes.Buffer

	TotalBytesWritten atomic.Uint64
	_                 [64 - unsafe.Sizeof(atomic.Uint64{})]byte
	NumberWrites      atomic.Uint64
	_                 [64 - unsafe.Sizeof(atomic.Uint64{})]byte
}

func (w *CountWriterWithBuffer) Write(payload []byte) (int, error) {
	w.TotalBytesWritten.Add(uint64(len(payload)))
	w.NumberWrites.Add(1)

	return w.Buf.Write(payload)
}

func (w *CountWriterWithBuffer) Reset() {
	w.TotalBytesWritten.Store(0)
	w.NumberWrites.Store(0)

	// w.Buf.Reset() throws away the data but retains the underlying slice storage.
	// This ensures subsequent benchmark runs won't pay a re-allocation penalty.
	w.Buf.Reset()
}
