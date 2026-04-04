package bytearena

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestArenaLayout(t *testing.T) {
	var a arena

	// Each hot atomic must start on a 64-byte boundary.
	offCursor := unsafe.Offsetof(a.cursor)
	offNumberWriters := unsafe.Offsetof(a.numberWriters)
	offRollbackCounter := unsafe.Offsetof(a.rollbackCounter)

	require.Zero(t,
		offCursor%64 != 0,

		"cursor offset %d is not 64-byte aligned",
		offCursor,
	)

	require.Zero(t,
		offNumberWriters%64 != 0,

		"numberWriters offset %d is not 64-byte aligned",
		offNumberWriters,
	)

	require.Zero(t,
		offRollbackCounter%64 != 0,

		"rollbackCounter offset %d is not 64-byte aligned",
		offRollbackCounter,
	)

	// Each hot atomic must be exactly 64 bytes apart from the next.
	require.EqualValues(t,
		64,
		offNumberWriters-offCursor,

		"cursor→numberWriters gap is %d, want 64",
		offNumberWriters-offCursor,
	)

	require.EqualValues(t,
		64,
		offRollbackCounter-offNumberWriters,

		"numberWriters→rollbackCounter gap is %d, want 64",
		offRollbackCounter-offNumberWriters,
	)
}
