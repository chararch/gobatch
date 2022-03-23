package logs

import (
	"fmt"
	"testing"
)

func Test_fileLine(t *testing.T) {
	fmt.Printf(fileLine())
}

func Test_logBase(t *testing.T) {
	fmt.Printf(logBase(Info))
}
