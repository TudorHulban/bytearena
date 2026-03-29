package helpers

import (
	"sync"
)

// ClosingWriter simulates a writer that becomes closed after a number of
// writes. All subsequent writes return io.ErrClosedPipe.
//
// Correct caller behavior:
//
// - Handle io.ErrClosedPipe.
//
// - Never assume a writer remains valid forever.
type ClosingWriter struct {
	mu sync.Mutex

	maxWrites     int
	currentWrites int
}

func NewClosingWriter(maxWrites int) *ClosingWriter {
	return &ClosingWriter{
		maxWrites: maxWrites,
	}
}

func (w *ClosingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentWrites >= w.maxWrites {
		return 0,
			ErrWriterIsClosed
	}

	w.currentWrites++

	return len(p), nil
}
