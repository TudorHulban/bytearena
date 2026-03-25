package helpers

import "unsafe"

// intLen returns the number of characters needed to print v.
func intLen(v int) int {
	if v == 0 {
		return 1
	}

	n := 0
	if v < 0 {
		n++
		v = -v
	}

	for v > 0 {
		n++
		v /= 10
	}

	return n
}

// writeInt writes v into dst and returns number of bytes written.
func writeInt(destination []byte, value int) int {
	if value == 0 {
		destination[0] = '0'

		return 1
	}

	start := 0

	if value < 0 {
		destination[0] = '-'
		start = 1
		value = -value
	}

	// Count digits
	tmp := value
	n := 0

	for tmp > 0 {
		n++
		tmp = tmp / 10
	}

	// Write digits backwards
	pos := start + n - 1

	for value > 0 {
		destination[pos] = byte('0' + value%10)
		value = value / 10
		pos--
	}

	return start + n
}

// SprintfInt is a minimal %d-only formatter optimized for speed.
// It performs two passes: first computes output size, then fills a preallocated buffer.
// It uses unsafe.String to avoid allocating when converting []byte to string.
// Returned string aliases the buffer, so the buffer must never be mutated afterwards.
func SprintfInt(format string, a ...int) string {
	indexArguments := 0
	lengthOutput := 0
	lengthFormat := len(format)

	// First pass: compute output size
	for ix := 0; ix < lengthFormat; ix++ {
		if format[ix] == '%' && ix+1 < lengthFormat && format[ix+1] == 'd' && indexArguments < len(a) {
			v := a[indexArguments]

			lengthOutput += intLen(v)
			indexArguments++
			ix++
		} else {
			lengthOutput++
		}
	}

	if lengthOutput == 0 {
		return ""
	}

	// Second pass: fill buffer
	buf := make([]byte, lengthOutput)
	out := 0
	indexArguments = 0

	for ix := 0; ix < lengthFormat; ix++ {
		if format[ix] == '%' && ix+1 < lengthFormat && format[ix+1] == 'd' && indexArguments < len(a) {
			v := a[indexArguments]

			out = out + writeInt(buf[out:], v)
			indexArguments++
			ix++
		} else {
			buf[out] = format[ix]
			out++
		}
	}

	return unsafe.String(&buf[0], lengthOutput)
}
