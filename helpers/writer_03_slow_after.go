package helpers

import (
	"sync"
	"time"
)

// SlowAfterWriter is an io.Writer that writes at full speed for a number of
// initial writes. After that it sleeps for a configured duration for a number
// of writes, then returns to fast mode.
//
// This simulates network congestion, rate limiting, or overloaded disks.
//
// Correct caller behavior:
//
// - Never assume latency is stable.
//
// - Always allow for slow writes.
//
// - Use deadlines or context if latency matters.
type SlowAfterWriter struct {
	mu sync.Mutex

	sleepDuration time.Duration

	fastWrites    int
	slowWrites    int
	currentWrites int
}

func NewSlowAfterWriter(fastWrites, slowWrites int, sleep time.Duration) *SlowAfterWriter {
	return &SlowAfterWriter{
		fastWrites:    fastWrites,
		slowWrites:    slowWrites,
		sleepDuration: sleep,
	}
}

func (w *SlowAfterWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentWrites >= w.fastWrites && w.currentWrites < w.fastWrites+w.slowWrites {
		time.Sleep(w.sleepDuration)
	}

	w.currentWrites++

	return len(p), nil
}
