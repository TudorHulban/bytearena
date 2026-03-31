package helpers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Goroutine producing values:
//
//	0–9:  fluctuating
//
//	10–14: reduced fluctuations
//
//	15–20: stable
var sequence = []int{
	1, 2, 5, 3, 2, 2, 8, 5, 2, 3, 7, 7, 8, 7, 8, 9, 9, 9, 9, 9, 9,
}

func TestDetectNoStabilization(t *testing.T) {
	pauseDuration := 100 * time.Microsecond
	pauseFn := func() {
		time.Sleep(pauseDuration)
	}

	// Channel simulating incoming values
	chData := make(chan int, 32)

	go func() {
		for _, value := range sequence {
			chData <- value
		}

		close(chData)
	}()

	// Reader function
	getValue := func() int {
		value, canRead := <-chData
		if !canRead {
			return -1 // channel closed
		}

		return value
	}

	params := ParamsDetectStabilization[int]{
		InitialValue:    getValue(),
		GetCurrentValue: getValue,
		PauseFn:         pauseFn,
		PauseFnDuration: pauseDuration,

		NumberStableSamples:  3,
		MaximumNumberSamples: 10,
	}

	timestamp, isStable := DetectStabilization(params)
	require.False(t,
		isStable,
		"stabilization expected only in the second half",
	)
	require.Zero(t, timestamp)
}

func TestDetectStabilization(t *testing.T) {
	pauseDuration := 100 * time.Microsecond
	pauseFn := func() {
		time.Sleep(pauseDuration)
	}

	// Channel simulating incoming values
	chData := make(chan int, 32)

	go func() {
		for _, value := range sequence {
			chData <- value
		}

		close(chData)
	}()

	// Reader function
	getValue := func() int {
		value, canRead := <-chData
		if !canRead {
			return -1 // channel closed
		}

		return value
	}

	params := ParamsDetectStabilization[int]{
		InitialValue:    getValue(),
		GetCurrentValue: getValue,
		PauseFn:         pauseFn,
		PauseFnDuration: pauseDuration,

		NumberStableSamples:  3,
		MaximumNumberSamples: 20,
	}

	timestamp, isStable := DetectStabilization(params)
	require.True(t,
		isStable,
		"no stabilization expected for the first half",
	)
	require.NotZero(t, timestamp)
}
