package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		logError("ERROR: exactly one argument must be provided, the restore's UID")
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
		logError("ERROR reading /restores directory: %s\n", err)
		return false
	}

	for _, child := range children {
		if !child.IsDir() {
			fmt.Printf("%s is not a directory, skipping.\n", child.Name())
			continue
		}

		_, err := os.Stat("/restores/" + child.Name() + "/.ark/" + os.Args[1])
		switch {
		case err == nil:
			fmt.Printf("Found /restores/%s/.ark/%s", child.Name(), os.Args[1])
		case os.IsNotExist(err):
			fmt.Printf("Not found: /restores/%s/.ark/%s\n", child.Name(), os.Args[1])
			return false
		default:
			logError("Error looking for /restores/%s/.ark/%s: %s\n", child.Name(), os.Args[1], err)
			return false
		}
	}

	return true
}

func logError(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
}
