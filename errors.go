package bytearena

import "errors"

var (
	ErrWriteNoActiveArena       = errors.New("write: no active arena")
	ErrWriteActiveArenaMismatch = errors.New("write: active arena mismatch")
	ErrWriteSubRegionFull       = errors.New("write: subregion full")

	ErrWriteMessageTooLarge = errors.New("write: message too large")
	ErrWriteShuttingDown    = errors.New("write: shutting down")
	ErrWriteBackpressure    = errors.New("write: backpressure")

	ErrWriterNoProgress  = errors.New("writer: no progress")
	ErrDroppedSealedData = errors.New("droped: sealed arena data")
)

type errorType uint64

const (
	TErrWriteNoActiveArena       errorType = iota + 1
	TErrWriteActiveArenaMismatch           // Arena was full
	TErrWriteSubRegionFull                 // Entry bigger than arena sub region

	TErrWriteMessageTooLarge // Consumer too slow
	TErrWriteShuttingDown    // Catch-all
	TErrWriteBackpressure

	TErrDeadlineExceeded

	TErrWriterNoProgress
	TErrDroppedSealedData
	TErrUnknown

	maxErrorTypes // Helper for array size
)

var errorTypeNames = [maxErrorTypes]string{
	TErrWriteNoActiveArena:       "no_active_arena",
	TErrWriteActiveArenaMismatch: "active_arena_mismatch",
	TErrWriteSubRegionFull:       "arena_subregion_full",

	TErrWriteMessageTooLarge: "message_too_large",
	TErrWriteShuttingDown:    "shutting_down",
	TErrWriteBackpressure:    "backpressure",

	TErrDeadlineExceeded: "deadline_exceeded",

	TErrWriterNoProgress:  "writer: no progress",
	TErrDroppedSealedData: "droped: sealed arena data",
	TErrUnknown:           "unknown",
}
