package helpers

import (
	"runtime"
	"sync/atomic"

	_ "unsafe"
)

// Thread safety is achieved through a two-layer defense mechanism:
// 1. runtime.procPin(): When a goroutine calls procPin(),
// the Go runtime scheduler locks that goroutine to its current logical processor thread ($P$) and disables preemption.
// This ensures that no other goroutine can jump onto the same thread
// and intercept the slot calculations while this function is executing.

// 2. atomic.AddUint64(): Since multiple goroutines can be pinned
// to different operating system threads that happen to map to the same CPU core slot (or if GOMAXPROCS causes index collisions),
// the actual mutation of the memory address uses a low-level atomic hardware instruction.
// This guarantees that concurrent operations on the same slot never suffer from dirty writes or lost increments.

//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin()

type PaddedSlot struct {
	value   uint64
	padding [7]uint64 // 64-byte alignment to prevent false sharing
}

type CPUCounter struct {
	slots []PaddedSlot
}

func NewCPUCounter() *CPUCounter {
	return &CPUCounter{
		slots: make([]PaddedSlot, runtime.GOMAXPROCS(0)),
	}
}

// Next matches your exact DX: It increments the local shard
// and immediately returns the new bouncing value for your round-robin.
func (c *CPUCounter) Next() uint64 {
	// 1. Temporarily pin the current goroutine to the OS thread/logical processor (P).
	// This prevents the Go scheduler from migrating this execution context to another core
	// mid-calculation and returns a stable Processor ID (pid).
	pid := procPin()

	// 2. Map the processor ID safely to the slot array boundaries using modulo math.
	idx := pid % len(c.slots)

	// 3. Atomically increment the local isolated shard slot.
	// Because the target PaddedSlot is aligned to its own hardware cache line and is
	// predominantly hit by the physical core hosting this P, the CPU finds the memory
	// instantly in its ultra-fast local L1 cache. It bypasses the global cross-core bus
	// cache-invalidation broadcast entirely.
	newValue := atomic.AddUint64(&c.slots[idx].value, 1)

	procUnpin()

	return newValue
}

// atomic.AddUint64 is only expensive when crossing physical boundaries to coordinate with other cores.
// By sharding the array and padding elements to match the CPU cache line size (preventing False Sharing),
// we convert a costly global hardware synchronization event into a local, core-isolated L1 cache update.
// For the architectural pattern, see: Thread-per-Core (TPC) or Shared-Nothing Architecture.
