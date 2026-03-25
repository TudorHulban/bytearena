package bytearena

import "errors"

var (
	ErrWriteNoActiveArena       = errors.New("write: no active arena")
	ErrWriteActiveArenaMismatch = errors.New("write: active arena mismatch")
	ErrWriteArenaFull           = errors.New("write: arena full")
	ErrWriteMessageTooLarge     = errors.New("write: message too large")
	ErrWriteShuttingDown        = errors.New("write: shutting down")
	ErrWriteBackpressure        = errors.New("write: backpressure")
)
