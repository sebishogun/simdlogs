// Command simdlogs is the log-database server. This entry point currently
// reports the instruction-set tier the simd kernels selected; the server
// wiring lands with the API phase.
package main

import (
	"fmt"

	"github.com/sebishogun/simd"
)

func main() {
	fmt.Println("simdlogs — simd tier:", simd.Tier())
}
