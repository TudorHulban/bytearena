package helpers

import (
	"fmt"
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

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkSprintfInt/1._empty_format-16         	571454272	         2.063 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSprintfInt/2._percent_but_no_d-16     	84472520	        13.97 ns/op	       2 B/op	       1 allocs/op
// BenchmarkSprintfInt/3._percent_d_but_no_args-16         	83182386	        13.60 ns/op	       2 B/op	       1 allocs/op
// BenchmarkSprintfInt/4._fewer_args_than_placeholders-16  	54669860	        21.01 ns/op	       4 B/op	       1 allocs/op
// BenchmarkSprintfInt/5._single_placeholder-16            	38617876	        32.29 ns/op	       8 B/op	       1 allocs/op
// BenchmarkSprintfInt/6._multiple_placeholders-16         	35945548	        31.84 ns/op	       5 B/op	       1 allocs/op
// BenchmarkSprintfInt/7._interleaved-16                   	40689417	        28.43 ns/op	       5 B/op	       1 allocs/op
// BenchmarkSprintfInt/8._negative_ints-16                 	37767238	        30.63 ns/op	       8 B/op	       1 allocs/op
// BenchmarkSprintfInt/9._no_placeholders-16               	80175160	        14.07 ns/op	       3 B/op	       1 allocs/op
// BenchmarkSprintfInt/10._invalid_percent_sequence-16     	68889207	        16.07 ns/op	       4 B/op	       1 allocs/op
// BenchmarkSprintfInt/11._mixed_valid_and_invalid-16      	38229586	        29.89 ns/op	       8 B/op	       1 allocs/op

func BenchmarkSprintfInt(b *testing.B) {
	b.ReportAllocs()

	tests := []struct {
		description string
		format      string
		args        []int
	}{
		// 1. Error-like cases
		{"1. empty format", "", nil},
		{"2. percent but no d", "%x", []int{42}},
		{"3. percent d but no args", "%d", nil},
		{"4. fewer args than placeholders", "%d %d", []int{7}},

		// 2. Normal cases
		{"5. single placeholder", "value=%d", []int{10}},
		{"6. multiple placeholders", "%d-%d-%d", []int{1, 2, 3}},
		{"7. interleaved", "A%dB%dC", []int{9, 8}},
		{"8. negative ints", "%d %d", []int{-1, -200}},
		{"9. no placeholders", "abc", []int{1, 2, 3}},
		{"10. invalid percent sequence", "x%yz", []int{5}},
		{"11. mixed valid and invalid", "%d %q %d", []int{1, 2}},
	}

	for _, tc := range tests {
		b.Run(
			tc.description,
			func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					out := SprintfInt(tc.format, tc.args...)
					_ = out
				}
			},
		)
	}
}

// cpu: AMD Ryzen 7 5800H with Radeon Graphics
// BenchmarkFmtSprintf/1._empty_format-16         	68209120	        17.30 ns/op	       0 B/op	       0 allocs/op
// BenchmarkFmtSprintf/2._percent_but_no_d-16     	26884616	        44.47 ns/op	       2 B/op	       1 allocs/op
// BenchmarkFmtSprintf/3._percent_d_but_no_args-16         	26502403	        43.88 ns/op	      16 B/op	       1 allocs/op
// BenchmarkFmtSprintf/4._fewer_args_than_placeholders-16  	18306931	        69.06 ns/op	      16 B/op	       1 allocs/op
// BenchmarkFmtSprintf/5._single_placeholder-16            	22201204	        52.57 ns/op	       8 B/op	       1 allocs/op
// BenchmarkFmtSprintf/6._multiple_placeholders-16         	13228581	        88.90 ns/op	       5 B/op	       1 allocs/op
// BenchmarkFmtSprintf/7._interleaved-16                   	15413917	        75.98 ns/op	       5 B/op	       1 allocs/op
// BenchmarkFmtSprintf/8._negative_ints-16                 	17407816	        67.55 ns/op	       8 B/op	       1 allocs/op
// BenchmarkFmtSprintf/9._no_placeholders-16               	 9771336	       122.6 ns/op	      32 B/op	       1 allocs/op
// BenchmarkFmtSprintf/10._invalid_percent_sequence-16     	16183243	        73.65 ns/op	      16 B/op	       1 allocs/op
// BenchmarkFmtSprintf/11._mixed_valid_and_invalid-16      	12678960	        91.66 ns/op	      24 B/op	       1 allocs/op

func BenchmarkFmtSprintf(b *testing.B) {
	b.ReportAllocs()

	tests := []struct {
		description string
		format      string
		args        []int
	}{
		// 1. Error-like cases
		{"1. empty format", "", nil},
		{"2. percent but no d", "%x", []int{42}},
		{"3. percent d but no args", "%d", nil},
		{"4. fewer args than placeholders", "%d %d", []int{7}},

		// 2. Normal cases
		{"5. single placeholder", "value=%d", []int{10}},
		{"6. multiple placeholders", "%d-%d-%d", []int{1, 2, 3}},
		{"7. interleaved", "A%dB%dC", []int{9, 8}},
		{"8. negative ints", "%d %d", []int{-1, -200}},
		{"9. no placeholders", "abc", []int{1, 2, 3}},
		{"10. invalid percent sequence", "x%yz", []int{5}},
		{"11. mixed valid and invalid", "%d %q %d", []int{1, 2}},
	}

	for _, tc := range tests {
		tc := tc

		// Prebuild []any once per test case to avoid per-iteration conversions.
		var anyArgs []any
		if len(tc.args) > 0 {
			anyArgs = make([]any, len(tc.args))
			for i, v := range tc.args {
				anyArgs[i] = v
			}
		}

		b.Run(
			tc.description,
			func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					_ = fmt.Sprintf(tc.format, anyArgs...)
				}
			},
		)
	}
}
