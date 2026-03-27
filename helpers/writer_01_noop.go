package helpers

type NoopWriter struct{}

func (NoopWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
