package helpers

import "sync"

// BlockingWriter is an io.Writer that performs normal writes for a limited
// number of calls. After the configured number of writes, all subsequent
// Write calls block forever and never return.
//
// This simulates a hung pipe, a dead TCP connection, or a stalled consumer.
//
// Correct caller behavior:
//
// - Always use context or deadlines when writing to any Writer that may block.
//
// - Never assume Write will return in finite time.
//
// - Never rely on goroutine leaks. Always cancel or close.
//
// Real world examples:
//
// - A TCP connection where the peer stops reading.
//
// - A pipe where the reader has died.
//
// - A slow disk that stops responding.
type BlockingWriter struct {
	mu sync.Mutex

	maxWrites     int
	currentWrites int
}

func NewBlockingWriter(maxWrites int) *BlockingWriter {
	return &BlockingWriter{
		maxWrites: maxWrites,
	}
}

func (w *BlockingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentWrites >= w.maxWrites {
		select {} // block forever
	}

	w.currentWrites++

	return len(p), nil
}
