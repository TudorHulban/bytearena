package helpers

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
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
	Buf bytes.Buffer

	sleepDuration time.Duration

	fastWrites    atomic.Uint64
	slowWrites    atomic.Uint64
	currentWrites atomic.Uint64

	mu sync.Mutex
}

var _ io.Writer = &SlowAfterWriter{}

func NewSlowAfterWriter(fastWrites, slowWrites uint64, sleepMiliseconds uint16) *SlowAfterWriter {
	result := SlowAfterWriter{
		fastWrites: atomic.Uint64{},
		slowWrites: atomic.Uint64{},

		sleepDuration: time.Millisecond + time.Duration(sleepMiliseconds),
	}

	result.fastWrites.Add(fastWrites)
	result.slowWrites.Add(slowWrites)

	return &result
}

func (w *SlowAfterWriter) Write(payload []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentWrites.Load() >= w.fastWrites.Load() && w.currentWrites.Load() < w.fastWrites.Load()+w.slowWrites.Load() {
		time.Sleep(w.sleepDuration)
	}

	w.currentWrites.Add(1)

	return w.Buf.Write(payload)
}
