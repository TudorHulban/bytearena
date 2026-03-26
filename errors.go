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

type errorType uint64

const (
	TErrWriteNoActiveArena       errorType = iota + 1
	TErrWriteActiveArenaMismatch           // Arena was full
	TErrWriteArenaFull                     // Entry bigger than arena
	TErrWriteMessageTooLarge               // Consumer too slow
	TErrWriteShuttingDown                  // Catch-all
	TErrWriteBackpressure
	TErrUnknown

	maxErrorTypes // Helper for array size
)

var errorTypeNames = map[errorType]string{
	TErrWriteNoActiveArena:       "no_active_arena",
	TErrWriteActiveArenaMismatch: "active_arena_mismatch",
	TErrWriteArenaFull:           "arena_full",
	TErrWriteMessageTooLarge:     "message_too_large",
	TErrWriteShuttingDown:        "shutting_down",
	TErrWriteBackpressure:        "backpressure",
	TErrUnknown:                  "unknown",
}
