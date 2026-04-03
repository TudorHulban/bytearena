package helpers

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func RunParallel(t *testing.T, workers, noIterations int, fn func() error, allowedErrors ...error) {
	t.Helper()

	var (
		wg        sync.WaitGroup
		stop      atomic.Bool
		totalDone atomic.Int64 // Shared counter across all workers
	)

	isAllowed := func(err error) bool {
		for _, allowed := range allowedErrors {
			if errors.Is(err, allowed) {
				return true
			}
		}

		return false
	}

	for range workers {
		wg.Go(func() {
			for {
				// 1. Check if another worker triggered a hard stop
				if stop.Load() {
					return
				}

				// 2. Claim an iteration
				// .Add returns the new value; if it's > total, we are done.
				if totalDone.Add(1) > int64(noIterations) {
					return
				}

				if errRun := fn(); errRun != nil {
					if isAllowed(errRun) {
						continue
					}

					// Atomic swap ensures we only log the FIRST error that kills the run
					if !stop.Swap(true) {
						t.Errorf("parallel worker error: %v", errRun)
					}

					return
				}
			}
		},
		)
	}

	wg.Wait()
}
