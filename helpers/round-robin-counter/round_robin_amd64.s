#include "textflag.h"

// func fastInc(addr *int64) int64
//
// Plain MOV / INCQ / MOV — no LOCK prefix, no memory barrier.
// This is intentionally non-atomic. The race window is the ~1-2 cycle gap
// between the load and the store. On a 3 GHz core that is ~0.3-0.6 ns.
// Two goroutines must both be inside that window simultaneously for a
// collision to occur, making it extremely unlikely in practice.
//
//
// Stack frame: 0 bytes locals, 16 bytes args (8 ptr + 8 ret).
TEXT ·fastInc(SB),NOSPLIT,$0-16
    MOVQ    addr+0(FP), AX   // AX = &counter
    MOVQ    0(AX), CX        // CX = *counter          (load)
    INCQ    CX               // CX++                   (increment)
    MOVQ    CX, 0(AX)        // *counter = CX           (store, no LOCK)
    MOVQ    CX, ret+8(FP)    // return CX
    RET
