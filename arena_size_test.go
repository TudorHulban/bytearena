package bytearena

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		description string
		fn          Size
		want        string
	}{
		// 1. boundary / error-adjacent
		{
			description: "1: zero",
			fn:          func() uint32 { return 0 },
			want:        "0",
		},
		{
			description: "2: just below 100K",
			fn:          func() uint32 { return (100 << 10) - 1 },
			want:        strconv.Itoa((100 << 10) - 1),
		},

		// 2. K-range
		{
			description: "3: 100K",
			fn:          Size100K,
			want:        "100K",
		},
		{
			description: "4: 500K",
			fn:          Size500K,
			want:        "500K",
		},

		// 3. M-range
		{
			description: "5: 1M",
			fn:          Size1M,
			want:        "1M",
		},
		{
			description: "6: 2M",
			fn:          Size2M,
			want:        "2M",
		},
		{
			description: "7: 4M",
			fn:          Size4M,
			want:        "4M",
		},
		{
			description: "8: 8M",
			fn:          Size8M,
			want:        "8M",
		},
		{
			description: "9: 16M",
			fn:          Size16M,
			want:        "16M",
		},
	}

	for _, tc := range tests {
		t.Run(
			tc.description,
			func(t *testing.T) {
				t.Parallel()

				got := tc.fn.String()

				require.Equal(t, tc.want, got)
			},
		)
	}
}
