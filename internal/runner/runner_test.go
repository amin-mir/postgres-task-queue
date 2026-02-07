package runner

import (
	"fmt"
	"testing"
)

func TestRunner(t *testing.T) {
	var opts []Opt

	opts = nil

	for range opts {
		fmt.Println("new opt")
	}
}
