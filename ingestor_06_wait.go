package bytearena

import (
	"context"
	"runtime"
	"time"

	"github.com/tudorhulban/bytearena/helpers"
)

// waitForWriters blocks until writers-in-flight reaches zero.
// should be used in tick.
func (*Ingestor) waitForWriters(a *arena) {
	writers := &a.numberWriters

	spin := 0

	for writers.Load() != 0 {
		if spin < 30 {
			spin++

			helpers.Pause(16)

			continue
		}

		runtime.Gosched()
	}
}

func (*Ingestor) waitForWritersCtx(ctx context.Context, a *arena) bool {
	spin := 0

	for {
		if a.numberWriters.Load() == 0 {
			return true
		}

		if spin < 50 {
			spin++

			helpers.Pause(16)

			continue
		}

		select {
		case <-ctx.Done():
			return false
		default:
			runtime.Gosched()
		}

		time.Sleep(time.Microsecond)
	}
}
