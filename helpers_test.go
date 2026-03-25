package bytearena

import "math/rand"

// Helper function to generate random string
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"

	b := make([]byte, n)

	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return string(b)
}
