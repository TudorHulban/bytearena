package helpers

import (
	"bytes"
	"io"
	"sync"
	"sync/atomic"
)

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
	Buf bytes.Buffer

	maxWrites     atomic.Uint64
	currentWrites atomic.Uint64

	mu sync.Mutex
}

var _ io.Writer = &BlockingWriter{}

func NewBlockingWriter(maxWrites int) *BlockingWriter {
	result := BlockingWriter{
		maxWrites: atomic.Uint64{},
	}

	result.maxWrites.Add(uint64(maxWrites))

	return &result
}

func (w *BlockingWriter) Write(payload []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentWrites.Load() >= w.maxWrites.Load() {
		// blocks forever
		// A hung writer must never cause producers to block or leak goroutines.
		select {}
	}

	w.currentWrites.Add(1)

	return w.Buf.Write(payload)
}
