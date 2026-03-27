package bytearena

import "sync/atomic"

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

// Snapshot collects all non-zero error counts, resets them to zero,
// and returns them as a map for external reporting.
func (r *ErrorsRegistry) Snapshot() map[string]uint64 {
	stats := make(map[string]uint64)

	for ix := range maxErrorTypes {
		et := ix

		// Atomic Swap ensures we don't lose counts between the read and reset
		count := r.Counts[et].value.Swap(0)

		if count > 0 {
			name, exists := errorTypeNames[et]
			if !exists {
				name = "unregistered_error"
			}

			stats[name] = count
		}
	}

	return stats
}
