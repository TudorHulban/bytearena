package helpers

import (
	"runtime"
)

type PaddedSlot struct {
	value uint64
	_     [7]uint64 // 64-byte hardware cache line isolation
}

type CPUCounter struct {
	slots []PaddedSlot // Offset 0 (Takes up 24 bytes for Slice Header: Ptr, Len, Cap)
	mask  uint64       // Offset 24
}

func NewCPUCounter() *CPUCounter {
	numSlots := nextPowerOfTwo(runtime.GOMAXPROCS(0))
	slots := make([]PaddedSlot, numSlots)

	for i := range slots {
		slots[i].value = uint64(i)
	}

	return &CPUCounter{
		slots: slots,
		mask:  uint64(numSlots - 1),
	}
}

// Next is a clean Go method wrapper that the compiler can easily inline.
func (c *CPUCounter) Next() uint64 {
	return nextAsm(c)
}

// Prototype for the naked assembly function defined in counter_amd64.s
func nextAsm(c *CPUCounter) uint64

func nextPowerOfTwo(n int) int {
	if n <= 0 {
		return 1
	}

	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32

	return n + 1
}
