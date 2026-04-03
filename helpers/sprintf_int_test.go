package helpers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSprintfInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		description string
		format      string
		want        string

		args []int
	}{
		// Error cases first

		{
			description: "1. empty format and no args yields empty string",
			format:      "",
			args:        nil,
			want:        "",
		},
		{
			description: "2. format with percent but no d yields literal percent",
			format:      "%x",
			args:        []int{42},
			want:        "%x",
		},
		{
			description: "3. format with percent d but no args leaves percent d literal",
			format:      "%d",
			args:        nil,
			want:        "%d",
		},
		{
			description: "4. format with percent d but fewer args than placeholders leaves remaining literal",
			format:      "%d %d",
			args:        []int{7},
			want:        "7 %d",
		},

		// Success cases

		{
			description: "5. single percent d with one argument",
			format:      "value=%d",
			args:        []int{10},
			want:        "value=10",
		},
		{
			description: "6. multiple percent d placeholders",
			format:      "%d-%d-%d",
			args:        []int{1, 2, 3},
			want:        "1-2-3",
		},
		{
			description: "7. interleaved literals and percent d",
			format:      "A%dB%dC",
			args:        []int{9, 8},
			want:        "A9B8C",
		},
		{
			description: "8. negative integers",
			format:      "%d %d",
			args:        []int{-1, -200},
			want:        "-1 -200",
		},
		{
			description: "9. no placeholders yields literal format",
			format:      "abc",
			args:        []int{1, 2, 3},
			want:        "abc",
		},
		{
			description: "10. percent not followed by d is literal",
			format:      "x%yz",
			args:        []int{5},
			want:        "x%yz",
		},
		{
			description: "11. mixed valid and invalid percent sequences",
			format:      "%d %q %d",
			args:        []int{1, 2},
			want:        "1 %q 2",
		},
	}

	for _, tc := range tests {
		t.Run(
			tc.description,
			func(t *testing.T) {
				t.Parallel()

				got := SprintfInt(tc.format, tc.args...)
				require.Equal(t, tc.want, got)
			},
		)
	}
}
