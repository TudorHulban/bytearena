//go:build linux && amd64

#include "textflag.h"

// func getg() unsafe.Pointer
TEXT ·getg(SB), NOSPLIT|NOFRAME, $0-8
	MOVQ (TLS), AX        // Load current g pointer from TLS
	MOVQ AX, ret+0(FP)
	RET
