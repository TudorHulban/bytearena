package helpers

// ErrorAfterWriter writes almost the entire payload and then returns an error.
// This simulates a real world writer that makes progress but still fails.
//
// Correct caller behavior:
//
// - Always check n even when err is non nil.
//
// - Never discard partial progress.
//
// - Retry from p[n:] if appropriate.
//
// - Never assume Write either fully succeeds or fully fails.
type ErrorAfterWriter struct {
	percentPartialWrite float64 // fraction of bytes to write before error, e.g. 0.99
}

func NewErrorAfterWriter() *ErrorAfterWriter {
	return &ErrorAfterWriter{
		percentPartialWrite: 0.99,
	}
}

func (w *ErrorAfterWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, ErrPartialWrite
	}

	n := int(float64(len(p)) * w.percentPartialWrite)
	if n < 1 {
		n = 1
	}

	if n > len(p) {
		n = len(p)
	}

	return n, ErrPartialWrite
}
