package helpers

import "errors"

// temporaryError implements net.Error for retryable failures.
type temporaryError struct {
	msg string
}

func (e *temporaryError) Error() string { return e.msg }
func (*temporaryError) Temporary() bool { return true }
func (*temporaryError) Timeout() bool   { return false }

var (
	ErrPartialWrite   = errors.New("simulated error after partial write")
	ErrWriterIsClosed = errors.New("writer is closed")

	ErrEINTR  = &temporaryError{msg: "interrupted system call"}
	ErrEAGAIN = &temporaryError{msg: "resource temporarily unavailable"}
)
