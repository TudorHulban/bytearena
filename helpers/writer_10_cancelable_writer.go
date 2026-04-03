package helpers

import (
	"context"
	"io"
	"time"
)

// CancelableWriter uses an io.Pipe as a cancellation boundary.
// The pipe has a critical property: closing the writer end forces the reader
// to unblock, and closing the reader forces the writer to unblock.
//
// This wrapper launches two goroutines:
//   1. A forwarder: io.Copy(dst, pipeReader)
//   2. A writer:   pipeWriter.Write(p)
//
// If the underlying dst.Write blocks forever, the forwarder goroutine would
// normally leak. But when the timeout expires the pipeWriter closes with an
// error. Closing pipeWriter causes:
//
//   - pipeWriter.Write to return immediately
//   - pipeReader.Read in the forwarder to return immediately
//
// Both goroutines exit deterministically, regardless of whether writer.Write is
// cooperative or permanently blocked.
//
// This provides a drop-in io.Writer that cannot block forever.

// CancelableWriter wraps an io.Writer and ensures that Write never blocks forever.
// If the underlying writer blocks, the wrapper forces unblocking via a pipe boundary.
type CancelableWriter struct {
	writer  io.Writer
	timeout time.Duration
}

// NewCancelableWriter returns a writer that will forcibly unblock after timeout.
func NewCancelableWriter(w io.Writer, timeout time.Duration) *CancelableWriter {
	return &CancelableWriter{
		writer:  w,
		timeout: timeout,
	}
}

func (w *CancelableWriter) Write(payload []byte) (int, error) {
	pipeReader, pipeWriter := io.Pipe()

	// Forwarder: copies from pipeReader → writer.
	forwardDone := make(chan struct{})

	var (
		errForward     error
		bytesForwarded int64
	)

	go func() {
		bytesForwarded, errForward = io.Copy(w.writer, pipeReader)
		_ = pipeReader.CloseWithError(errForward)

		close(forwardDone)
	}()

	// Writer goroutine: writes payload into the pipe.
	go func() {
		_, _ = pipeWriter.Write(payload)

		pipeWriter.Close()
	}()

	// Timeout acts as internal cancellation.
	timer := time.NewTimer(w.timeout)
	defer timer.Stop()

	select {
	case <-forwardDone:
		return int(bytesForwarded), errForward

	case <-timer.C:
		// Force both goroutines to unblock.
		_ = pipeWriter.CloseWithError(context.DeadlineExceeded)

		return 0, context.DeadlineExceeded
	}
}
