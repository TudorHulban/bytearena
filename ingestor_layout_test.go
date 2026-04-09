package bytearena

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestIngestorLayout(t *testing.T) {
	var item Ingestor

	// Hot fields must be on separate cache lines.
	// fieldalignment govet warning is intentionally suppressed —
	// the [56]byte pads are load-bearing, not waste.
	activeOff := unsafe.Offsetof(item.active)
	counterOff := unsafe.Offsetof(item.counterRequests)
	stoppedOff := unsafe.Offsetof(item.isStopped)

	assert.Equal(t, uintptr(0), activeOff%64, "active not cache-line aligned")
	assert.Equal(t, uintptr(0), counterOff%64, "counterRequests not cache-line aligned")
	assert.Equal(t, uintptr(0), stoppedOff%64, "isStopped not cache-line aligned")

	// No two hot fields share a cache line.
	assert.NotEqual(t, activeOff/64, counterOff/64)
	assert.NotEqual(t, counterOff/64, stoppedOff/64)
}
