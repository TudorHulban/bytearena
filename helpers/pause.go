package helpers

// pause performs a tiny, inlineable delay that the compiler cannot remove.
// It costs ~1–2 ns depending on CPU and Go version.
func Pause(noIterations uint16) {
	// Prevents the compiler from optimizing the loop away.
	for i := range noIterations {
		_ = i
	}
}
