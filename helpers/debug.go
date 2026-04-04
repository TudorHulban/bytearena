package helpers

import (
	"fmt"
	"runtime"
)

// Use as defer helpers.TraceExit().
func TraceExit() {
	pc, _, line, ok := runtime.Caller(1) // Get the caller of this function
	if ok {
		fmt.Printf(
			"exiting function %s at line %d.\n",

			runtime.FuncForPC(pc).Name(),
			line,
		)
	}
}
