package helpers

// ZeroProgressWriter returns (0, nil) for every Write call.
//
// This simulates a buggy writer or a rate limited writer that refuses to
// make progress but does not block.
//
// Correct caller behavior:
//
// - Detect zero progress and avoid infinite loops.
//
// - Implement a maximum retry count or timeout.
type ZeroProgressWriter struct{}

func (*ZeroProgressWriter) Write(p []byte) (int, error) {
	return 0, nil
}
