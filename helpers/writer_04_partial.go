package helpers

import (
	"sync"
)

// PartialWriter performs full writes for a number of initial writes.
// After that it performs partial writes based on a percentage of the
// provided payload. After the configured number of partial writes it
// returns to full writes.
//
// This simulates TCP sockets, pipes, or buffered writers that cannot
// accept the entire buffer.
//
// Correct caller behavior:
// - Always handle partial writes.
// - Never assume Write writes the entire buffer.
// - Always loop until the entire buffer is written or an error occurs.
type PartialWriter struct {
	mu sync.Mutex

	fullWrites int
	partWrites int
	count      int

	percentPartialWrite float64
}

func NewPartialWriter(fullWrites, partWrites int, percent float64) *PartialWriter {
	return &PartialWriter{
		fullWrites:          fullWrites,
		partWrites:          partWrites,
		percentPartialWrite: percent,
	}
}

func (w *PartialWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.count >= w.fullWrites && w.count < w.fullWrites+w.partWrites {
		if len(p) == 0 {
			w.count++

			return 0, nil
		}

		n := int(float64(len(p)) * w.percentPartialWrite)
		if n < 1 {
			n = 1
		}

		if n > len(p) {
			n = len(p)
		}

		w.count++

		return n, nil
	}

	w.count++

	return len(p), nil
}
