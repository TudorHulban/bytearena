//go:build amd64 && linux

package bytearena

import (
	"runtime"
	"time"

	"github.com/tudorhulban/bytearena/helpers"
)

func (ing *Ingestor) waitForWriters(a *arena) error {
	writers := &a.numberWriters
	spin := 0

	// Compute deadline once.
	deadline := helpers.Nanotime() + int64(ing.millisecondsUnblock)*1_000_000

	// Adaptive backoff.
	for writers.Load() != 0 {
		switch {
		case spin < 20:
			helpers.Pause(1)

			spin++
			continue // ← skip Nanotime entirely in the hot phase

		case spin < 100:
			runtime.Gosched()

		default:
			time.Sleep(5 * time.Microsecond)
		}

		spin++

		if helpers.Nanotime() > deadline {
			return errTimeoutWaitForWriters
		}
	}

	return nil
}
