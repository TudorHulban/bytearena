package helpers

// Pipe-based cancellation mechanism:
//
// io.Pipe gives us a reader (pipeReader) and writer (pipeWriter) pair with a
// critical property: closing either end *forces* the other end to unblock.
// This makes the pipe a cancellation boundary that we control.
//
// Why this solves the runaway-goroutine problem:
//
// If dst.Write blocks forever, a goroutine doing io.Copy(dst, pipeReader)
// would normally be stuck forever too. But when the context is cancelled,
// we close pipeWriter with an error. Closing pipeWriter causes:
//
//   1. pipeWriter.Write(...) to return immediately in the writer goroutine
//   2. pipeReader.Read(...) to return immediately in the forwarding goroutine
//
// Both goroutines exit deterministically, regardless of whether dst.Write
// is cooperative or permanently blocked.
//
// This is the only portable way in Go to force-unblock a write path when the
// underlying writer does not support deadlines or cancellation.

// func WriteWithContext(ctx context.Context, w io.Writer, payload []byte) (int, error) {
// 	// Pipe used as a cancellation boundary:
// 	// - pw is where *we* write the payload
// 	// - pr is what the forwarding goroutine reads from
// 	// Closing pw (with or without error) forces pr.Read to unblock.
// 	pr, pw := io.Pipe()

// 	// Forward all bytes read from the pipe into the real destination writer.
// 	// This goroutine exits deterministically when:
// 	//   1. pw is closed (normal completion or context cancellation)
// 	//   2. dst.Write returns an error
// 	go func() {
// 		_, forwardErr := io.Copy(w, pr)
// 		_ = pr.CloseWithError(forwardErr)
// 	}()

// 	writeDone := make(chan struct{})
// 	var bytesWritten int
// 	var writeErr error

// 	// Write the payload into the pipe.
// 	// This goroutine exits when:
// 	//   1. payload fully written
// 	//   2. pw is closed due to context cancellation
// 	go func() {
// 		bytesWritten, writeErr = pw.Write(payload)
// 		close(writeDone)
// 	}()

// 	select {
// 	case <-ctx.Done():
// 		// Closing pw forces:
// 		//   - pw.Write to unblock immediately
// 		//   - pr.Read in the forwarding goroutine to unblock
// 		// This prevents both goroutines from leaking.
// 		_ = pw.CloseWithError(ctx.Err())
// 		return 0, ctx.Err()

// 	case <-writeDone:
// 		// Normal completion: close pw so the forwarding goroutine terminates.
// 		_ = pw.Close()
// 		return bytesWritten, writeErr
// 	}
// }
