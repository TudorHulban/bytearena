package bytearena

import (
	"context"
	"runtime"

	"github.com/tudorhulban/bytearena/helpers"
)

// waitForWriters blocks until writers-in-flight reaches zero.
// should be used in tick.
func (*Ingestor) waitForWriters(a *arena) bool {
	writers := &a.numberWriters
	spin := 0

	for {
		if writers.Load() == 0 {
			return true
		}

		if spin < 20 {
			spin++

			helpers.Pause(1)

			continue
		}

		runtime.Gosched()

		return false
	}
}

// waitForWritersCtx provides cooperative wait with cancellation.
func (i *Ingestor) waitForWritersCtx(ctx context.Context, a *arena) {
	for !i.waitForWriters(a) {
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
