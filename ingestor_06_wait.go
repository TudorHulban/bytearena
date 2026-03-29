package bytearena

import (
	"runtime"

	"github.com/tudorhulban/bytearena/helpers"
)

// waitForWriters blocks until writers-in-flight reaches zero.
// should be used in tick.
func (*Ingestor) waitForWriters(a *arena) {
	writers := &a.numberWriters

	spin := 0

	for writers.Load() != 0 {
		if spin < 20 {
			spin++

			helpers.Pause(1)

			continue
		}

		runtime.Gosched()
	}
}
