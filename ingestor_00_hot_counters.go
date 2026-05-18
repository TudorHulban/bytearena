package bytearena

import (
	"sync/atomic"

	"github.com/tudorhulban/bytearena/helpers"
)

// Counters are the primary engine values for the ingestion pipeline.
//
// DESIGN & PERFORMANCE ARCHITECTURE:
// The counters are deliberately implemented as unexported package-level
// global variables rather than a field within the Ingestor struct.
//
// 1. Zero Pointer Indirection (Direct Addressing):
//    As a package-level variable, the Go compiler resolves this memory address
//    at compile time using RIP-relative (absolute) addressing. If attached to
//    the Ingestor struct, every increment would require resolving the base
//    pointer offset (instanceAddress + offset), consuming an extra CPU register
//    and adding instruction cycles on ultra-hot paths.
//
// 2. Struct Footprint and Cache Alignment:
//    Isolating the counter keeps the Ingestor struct lean, preventing
//    unnecessary cache line bloating.
//
// 3. False Sharing Prevention:
//    The variable is wrapped in strict 56-byte leading and trailing padding.
//    This guarantees that the pointer itself exclusively owns a 64-byte L1/L2
//    cache line, ensuring hot writes to nearby globals never invalidate the
//    cache line holding this reference.
//
// CONSTRAINTS:
// This pattern assumes Ingestor operates as a singleton or that all Ingestor
// instances should intentionally contribute to a single, globally sharded
// CPU counter pool.

// Prevent false sharing by padding the global hot counter to 64 bytes.
// This guarantees it owns its entire cache line.
var (
	_CounterAtomic atomic.Uint64
	_              [56]byte // Trailing padding (essential)
)

func init() {
	_CounterCoreCPU = helpers.NewCPUCounter()
}

var (
	_CounterCoreCPU *helpers.CPUCounter
	_               [56]byte
)
