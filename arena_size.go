package bytearena

import "fmt"

const (
	_Size1K = 1 << 10 // 1 KiB
)

type Size func() uint32

// String returns a human-readable representation of the size value
func (fn Size) String() string {
	val := fn()

	switch {
	case val >= 16<<20:
		return fmt.Sprintf("%dM", val/(1<<20))
	case val >= 8<<20:
		return fmt.Sprintf("%dM", val/(1<<20))
	case val >= 4<<20:
		return fmt.Sprintf("%dM", val/(1<<20))
	case val >= 2<<20:
		return fmt.Sprintf("%dM", val/(1<<20))
	case val >= 1<<20:
		return fmt.Sprintf("%dM", val/(1<<20))
	case val >= 500<<10:
		return fmt.Sprintf("%dK", val/(1<<10))
	case val >= 100<<10:
		return fmt.Sprintf("%dK", val/(1<<10))
	default:
		return fmt.Sprintf("%d", val)
	}
}

// Define the constants as functions
var (
	Size16M  Size = func() uint32 { return 16 << 20 }
	Size8M   Size = func() uint32 { return 8 << 20 }
	Size4M   Size = func() uint32 { return 4 << 20 }
	Size2M   Size = func() uint32 { return 2 << 20 }
	Size1M   Size = func() uint32 { return 1 << 20 }
	Size500K Size = func() uint32 { return 500 << 10 }
	Size100K Size = func() uint32 { return 100 << 10 }
)
