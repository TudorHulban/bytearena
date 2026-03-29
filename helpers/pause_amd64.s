#include "textflag.h"

// Add a comma before NOSPLIT
TEXT ·procyield(SB), NOSPLIT, $0-4
    MOVL cycles+0(FP), AX
loop:
    PAUSE
    SUBL $1, AX
    JNZ  loop
    RET
