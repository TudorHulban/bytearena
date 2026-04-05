package bytearena

import (
	"context"
	"errors"
	"sync/atomic"
)

// Injected by the Consumer during the Flush cycle if LossRegistry > 0
// {
//   "bytearena_meta": {
//     "type": "drop_report",
//     "dropped_full": 4502,
//     "dropped_flood": 0,
//     "timestamp": 1711404800
//   }
// }

type ErrorsRegistry struct {
	// A fixed array of padded atomics
	// Each entry is separated by padding to prevent false sharing
	Counts [maxErrorTypes]struct {
		value atomic.Uint64
		_     [56]byte // Padding to reach 64-byte cache line
	}
}

// Inc increments the counter for a specific error type.
// It is thread-safe, lock-free, and padded to prevent false sharing.
func (r *ErrorsRegistry) Inc(et errorType) {
	// Boundary check to prevent panic; TErrUnknown is the fallback.
	if et <= 0 || et >= maxErrorTypes {
		et = TErrUnknown
	}

	// We use the underlying index directly.
	// The CPU will handle the pointer math (index * 64 bytes).
	r.Counts[et].value.Add(1)
}

// LoadAndReset returns the current value and resets it to zero.
// Useful for the Consumer during the flush cycle.
func (r *ErrorsRegistry) LoadAndReset(et errorType) uint64 {
	if et <= 0 || et >= maxErrorTypes {
		return 0
	}

	return r.Counts[et].value.Swap(0)
}

func (r *ErrorsRegistry) load(et errorType) uint64 {
	if et <= 0 || et >= maxErrorTypes {
		return 0
	}

	return r.Counts[et].value.Load()
}

func (r *ErrorsRegistry) loadError(value error) {
	if value == nil {
		return
	}

	if errors.Is(value, context.DeadlineExceeded) {
		r.Counts[TErrDeadlineExceeded].value.Add(1)

		return
	}

	switch value {
	case ErrWriteNoActiveArena:
		r.Counts[TErrWriteNoActiveArena].value.Add(1)

	case ErrWriteActiveArenaMismatch:
		r.Counts[TErrWriteActiveArenaMismatch].value.Add(1)

	case ErrWriteSubRegionFull:
		r.Counts[TErrWriteArenaFull].value.Add(1)

	case ErrWriteMessageTooLarge:
		r.Counts[TErrWriteMessageTooLarge].value.Add(1)

	case ErrWriteShuttingDown:
		r.Counts[TErrWriteShuttingDown].value.Add(1)

	case ErrWriteBackpressure:
		r.Counts[TErrWriteBackpressure].value.Add(1)

	default:
		r.Counts[TErrUnknown].value.Add(1)
	}
}

// Snapshot collects all non-zero error counts, resets them to zero,
// and returns them as a map for external reporting.
func (r *ErrorsRegistry) Snapshot() map[string]uint64 {
	result := make(map[string]uint64)

	for ix := range maxErrorTypes {
		// Atomic Swap ensures counts between the read and reset are not lost.
		count := r.Counts[ix].value.Swap(0)

		if count > 0 {
			result[errorTypeNames[ix]] = count
		}
	}

	return result
}
