//go:build amd64 && linux

package helpers

func procyield(cycles uint32)

func Pause(n int) {
	if n > 0 {
		procyield(uint32(n))
	}
}
