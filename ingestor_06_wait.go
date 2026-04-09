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
		// Check cancellation EVERY iteration.
		// Use ctx.Err as faster than ctx.Done read.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// ✅ Adaptive backoff strategy
		switch {
		case spin < 20:
			helpers.Pause(1)

		case spin < 100:
			runtime.Gosched()

		default: // Long wait: sleep to avoid CPU burn under sustained contention.
			time.Sleep(5 * time.Microsecond)
		}

		spin++
	}

	return nil
}
