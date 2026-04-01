package helpers

import (
	"context"
	"io"
)

func WriteWithContext(ctx context.Context, w io.Writer, p []byte) (int, error) {
	type result struct {
		errWrite     error
		bytesWritten int
	}

	chDone := make(chan result, 1)

	go func() {
		n, err := w.Write(p)

		chDone <- result{
			bytesWritten: n,
			errWrite:     err,
		}
	}()

	select {
	case <-ctx.Done():
		return 0,
			ctx.Err()

	case results := <-chDone:
		return results.bytesWritten, results.errWrite
	}
}
