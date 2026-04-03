package helpers

import "strconv"

// MakePayload returns a byte slice of exactly n bytes, all set to char.
//
// char like 'x'
func MakePayload(length int, char byte) []byte {
	result := make([]byte, length)

	for i := range length {
		result[i] = char
	}

	return result
}

// MakePayloadNumbered returns a payload of exactly `length` bytes.
// The decimal representation of `number` is written first.
// If the number string is longer than length, it is truncated.
// The rest of the payload is filled with `char`.
func MakePayloadNumbered(length, number int, char byte) []byte {
	result := make([]byte, length)

	num := strconv.AppendInt(nil, int64(number), 10)
	n := len(num)

	if n > length {
		copy(result, num[:length])

		return result
	}

	copy(result, num)

	for i := n; i < length; i++ {
		result[i] = char
	}

	return result
}

func MakePayloadWLineFeed(length, number int, char byte) []byte {
	result := make([]byte, length)

	num := strconv.AppendInt(nil, int64(number), 10)
	n := len(num)

	// Reserve 1 byte for '\n'
	if n+1 > length {
		// Truncate number so that last byte is '\n'
		copy(result, num[:length-1])
		result[length-1] = '\n'

		return result
	}

	copy(result, num)

	for i := n; i < length-1; i++ {
		result[i] = char
	}

	result[length-1] = '\n'

	return result
}
