package helpers

import "time"

type ParamsDetectStabilization[T comparable] struct {
	InitialValue    T
	GetCurrentValue func() T

	PauseFn         func()
	PauseFnDuration time.Duration

	NumberStableSamples  uint16 // how many consecutive equal values = "stable"
	MaximumNumberSamples uint16 // safety limit
}

// DetectStabilization detects if a value becomes stable.
// If for NumberStableSamples consecutive observations the value does not change
// it is considered it stabilized.
//
// If the value stabilized it returns the timestamp of the first stable observation, reconstructed as:
//
//	t0 + stableStartIndex * pauseDuration
//
// If stabilization never occurs, it returns zero timestamp and false.
//
// Requirements:
//   - PauseFn must have a known, fixed duration (pauseDuration).
//   - pauseDuration must be provided explicitly.
func DetectStabilization[T comparable](params ParamsDetectStabilization[T]) (time.Time, bool) {
	if params.NumberStableSamples == 0 {
		params.NumberStableSamples = 1
	}

	if params.MaximumNumberSamples < params.NumberStableSamples {
		params.MaximumNumberSamples = params.NumberStableSamples
	}

	// Timestamp before sampling
	t0 := time.Now()

	last := params.InitialValue
	consecutive := uint16(0)
	stableStartIndex := int(-1)

	for ix := uint16(0); ix < params.MaximumNumberSamples; ix++ {
		params.PauseFn()

		current := params.GetCurrentValue()

		if current != last {
			// value changed → reset streak
			last = current
			consecutive = 0
			stableStartIndex = -1

			continue
		}

		// value same as previous sample
		if consecutive == 0 {
			stableStartIndex = int(ix) // first stable observation
		}

		consecutive++

		if consecutive >= params.NumberStableSamples {
			// reconstruct timestamp of first stable sample
			return t0.Add(time.Duration(stableStartIndex) * params.PauseFnDuration),
				true
		}
	}

	// never stabilized → return timestamp after sampling
	return time.Time{}, false
}
