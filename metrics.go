package bytearena

import "sync/atomic"

type Metrics struct {
	WritesSucceeded atomic.Uint64
	WritesFailed    atomic.Uint64
	Rotations       atomic.Uint64
	FlushedBytes    atomic.Uint64
}
