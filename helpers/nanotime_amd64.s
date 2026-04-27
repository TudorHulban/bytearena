#include "textflag.h"

TEXT ·Nanotime(SB), NOSPLIT, $0-8
    CALL runtime·nanotime(SB)
    MOVQ AX, ret+0(FP)
    RET
