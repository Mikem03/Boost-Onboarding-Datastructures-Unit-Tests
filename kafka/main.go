package main

import (
	"fmt"
	"os"
)

func main() {
	if err := startServer(); err != nil {
		fmt.Println("failed to start")
		os.Exit(1)
	}
}
