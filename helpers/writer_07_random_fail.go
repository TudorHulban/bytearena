package helpers

import (
	"math/rand"
	"sync"
)

// RandomFailWriter simulates transient failures such as EINTR or EAGAIN.
// Every failEvery writes, it returns a temporary error chosen at random.
//
// Correct caller behavior:
//
// - Detect net.Error and check Temporary().
//
// - Retry the write.
//
// - Apply backoff if needed.
//
// - Never assume the writer is broken permanently.
type RandomFailWriter struct {
	mu sync.Mutex

	failEvery int
	count     int
}

func NewRandomFailWriter(failEvery int) *RandomFailWriter {
	return &RandomFailWriter{failEvery: failEvery}
}

func (w *RandomFailWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.count++

	if w.count%w.failEvery == 0 {
		if rand.Intn(2) == 0 {
			return 0, ErrEINTR
		}

		return 0, ErrEAGAIN
	}

	return len(p), nil
}
