package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "ERROR: exactly one argument must be provided, the restore's UID")
		os.Exit(1)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if done() {
				fmt.Println("All restic restores are done")
				return
			}
		}
	}
}

// done returns true if for each directory under /restores, a file exists
// within the .ark/ subdirectory whose name is equal to os.Args[1], or
// false otherwise
func done() bool {
	children, err := ioutil.ReadDir("/restores")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR reading /restores directory: %s\n", err)
		return false
	}

	for _, child := range children {
		if !child.IsDir() {
			fmt.Printf("%s is not a directory, skipping.\n", child.Name())
			continue
		}

		doneFile := filepath.Join("/restores", child.Name(), ".ark", os.Args[1])

		if _, err := os.Stat(doneFile); os.IsNotExist(err) {
			fmt.Printf("Not found: %s\n", doneFile)
			return false
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR looking for %s: %s\n", doneFile, err)
			return false
		}

		fmt.Printf("Found %s", doneFile)
	}

	return true
}
