package main

import (
	"os"

	"github.com/lmzuccarelli/golang-oc-mirror-refactor/pkg/cli"
)

func main() {
	err := cli.Execute()
	if err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}
