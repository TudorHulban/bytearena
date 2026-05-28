package main

import (
	"fmt"

	"github.com/tudorhulban/bytearena/helpers"
)

// file to be used with the tags to test the next asm.

func main() {
	counter := helpers.NewCPUCounter()

	fmt.Println(
		"next counter value is:",
		counter.Next(),
	)
}
