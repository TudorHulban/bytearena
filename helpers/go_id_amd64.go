//go:build amd64 && go1.26 && !go1.27

package helpers

import "unsafe"

const goidOffset = 152 // stable for Go 1.26

// getg returns the current *g as unsafe.Pointer
// Implemented in goid_amd64.s
func getg() unsafe.Pointer

func GoroutineID() int64 {
	gp := getg()

	// Safer and cleaner than uintptr arithmetic
	return *(*int64)(unsafe.Add(gp, goidOffset))
}
