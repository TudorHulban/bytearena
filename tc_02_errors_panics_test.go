package bytearena

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test Case: Producer Panic Safety

// Test: Producer panics during write
// Verifies: writers counter is decremented even on panic
func TestWritePanicDoesNotLeak(t *testing.T) {
	ingestor, errCrIngestor := NewIngestor(_Size1K, &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	func() {
		defer func() {
			_ = recover()
		}()

		ingestor.write(
			100,
			func(dst []byte) {
				panic("some panic")
			},
		)
	}()

	active := ingestor.active.Load()
	require.Zero(t, active.numberWriters.Load())
}

func TestProducerInWritePanic(t *testing.T) {
	ingestor, errCrIngestor := NewIngestor(_Size1K, &bytes.Buffer{})
	require.NoError(t, errCrIngestor)
	require.NotNil(t, ingestor)

	// Use defer/recover to simulate panic in producer
	func() {
		defer func() {
			_ = recover()
		}()

		payload := t.Name()

		ingestor.write(
			uint32(len(payload)),

			func(_ []byte) {
				panic("simulated crash before end write")
			},
		)
	}()

	// writers should be zero as the write finishes with an endwrite.
	activeArena := ingestor.active.Load()
	require.Zero(t,
		activeArena.numberWriters.Load(),
		"blocked writer should be handled",
	)
}
