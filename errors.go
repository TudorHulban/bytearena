package bytearena

import "errors"

var (
	errWriteNoActiveArena       = errors.New("write: no active arena")
	errWriteActiveArenaMismatch = errors.New("write: active arena mismatch")
	errWriteSubRegionFull       = errors.New("write: subregion full")

	errWriteMessageTooLarge = errors.New("write: message too large")
	errWriteShuttingDown    = errors.New("write: shutting down")

	errWriterNoProgress      = errors.New("writer: no progress")
	errTimeoutWaitForWriters = errors.New("tick: timeout waiting for writers")
)

type errorType uint64

const (
	TErrWriteNoActiveArena       errorType = iota + 1
	TErrWriteActiveArenaMismatch           // Arena was full
	TErrWriteSubRegionFull                 // Entry bigger than arena sub region

	TErrWriteMessageTooLarge // Consumer too slow
	TErrWriteShuttingDown    // Catch-all

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

	TErrDeadlineExceeded: "deadline_exceeded",

	TErrWriterNoProgress:  "writer: no progress",
	TErrDroppedSealedData: "dropped: sealed arena data",
	TErrUnknown:           "unknown",
}
