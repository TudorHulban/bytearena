package bytearena

import (
	"context"
	"runtime"
	"time"

	"github.com/tudorhulban/bytearena/helpers"
)

// waitForWritersCtx blocks until writers==0 OR context expires.
// Uses adaptive backoff: spin → yield → sleep.
func (*Ingestor) waitForWritersCtx(ctx context.Context, a *arena) error {
	writers := &a.numberWriters
	spin := 0

	for writers.Load() != 0 {
		// ✅ Check cancellation EVERY iteration
		select {
		case <-ctx.Done():
			return ctx.Err() // context.DeadlineExceeded or Canceled

		default:
		}

		// ✅ Adaptive backoff strategy
		switch {
		case spin < 20:
			helpers.Pause(1)

			runtime.Gosched()

		case spin < 100:
			helpers.Pause(3)

			runtime.Gosched()

		default: // Long wait: sleep to avoid CPU burn under sustained contention.
			time.Sleep(10 * time.Microsecond)
		}

		spin++
	}

	return nil
}
