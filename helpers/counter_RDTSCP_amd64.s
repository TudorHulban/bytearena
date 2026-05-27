//go:build amd64 && legacy_cpu
#include "textflag.h"

// func nextAsm(c *CPUCounter) uint64
// Frame size declaration: $0 bytes local stack, 16 bytes for args/returns
// (8 bytes for the *CPUCounter pointer + 8 bytes for the uint64 return value)
TEXT ·nextAsm(SB), NOSPLIT, $0-16
    // Pull the object pointer 'c' from the stack into base register BX
    MOVQ c+0(FP), BX

    // 1. Read Logical Processor ID via RDTSCP
    // RDTSCP overwrites AX, DX, and CX.
    // AX and DX get the 64-bit timestamp, while CX gets the Processor ID.
    RDTSCP

    // 2. Map Processor ID to slot index using our power-of-two mask
    // CPUCounter structure offsets:
    // 0(BX)  = slots slice backing array pointer (8 bytes)
    // 24(BX) = mask value (8 bytes)
    MOVQ 24(BX), SI        // SI = c.mask
    ANDQ CX, SI            // SI = pid & mask (O(1) bounds-safe indexing)

    // 3. Calculate target slot memory address
    // Each PaddedSlot element is 64 bytes (2^6). 
    // We left-shift the index by 6 to find the exact byte offset.
    SHLQ $6, SI            // SI = index * 64
    MOVQ 0(BX), DI         // DI = c.slots.ptr (slice array base address)
    ADDQ SI, DI            // DI = base + offset (Direct pointer to c.slots[idx].value)

    // 4. Atomic Fetch-and-Add
    MOVQ $1, AX            // AX = 1 (increment delta)
    LOCK
    XADDQ AX, (DI)         // Atomically add AX to (DI). AX now holds the OLD value.
    ADDQ $1, AX            // Increment AX locally to yield the expected NEW value.

    // 5. Write the returned value to the stack frame allocation
    MOVQ AX, ret+8(FP)
    RET