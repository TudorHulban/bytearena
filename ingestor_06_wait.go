package bytearena

import (
	"context"
	"runtime"
	"time"
)

// waitForWriters blocks until writers-in-flight reaches zero.
// should be used in tick.
func (*Ingestor) waitForWriters(a *arena) {
	writers := &a.numberWriters

	spin := 0

	for writers.Load() != 0 {
		spin++

		if spin < 64 {
			continue
		}

		spin = 0

		runtime.Gosched()
	}
}

func (*Ingestor) waitForWritersCtx(ctx context.Context, a *arena) bool {
	spin := 0

	for {
		if a.numberWriters.Load() == 0 {
			return true
		}

		spin++

		if spin < 50 {
			runtime.Gosched()

			continue
		}

		select {
		case <-ctx.Done():
			return false
		default:
		}

		time.Sleep(time.Microsecond)
	}
}
