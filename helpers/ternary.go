package helpers

import "slices"

// Ternary returns first value if condition true.
// Second if condition false.
//
// Go eagerly evaluates arguments therefore arguments should be valid.
// Use the lazy version if unsure.
func Ternary[T any](condition bool, value1, value2 T) T {
	if condition {
		return value1
	}

	return value2
}

func TernaryLazy[T any](condition bool, value1, value2 func() T) T {
	if condition {
		return value1()
	}

	return value2()
}

func TernaryWithValueIn[V comparable, T any](haystack []V, needle V, value1, value2 T) T {
	if slices.Contains(haystack, needle) {
		return value1
	}

	return value2
}
