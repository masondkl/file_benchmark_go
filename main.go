package main

import (
	"fmt"
	"os"
	"time"
)

const (
	SIZE int = 1024 * 1024 * 1024
)

func main() {
	// Open file with read-write mode, create if not exists, and truncate
	file, err := os.OpenFile("example.txt", os.O_CREATE|os.O_SYNC, 0644)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	bytes := make([]byte, SIZE)

	start := time.Now().UnixNano()
	// Example: Write to the file
	_, err = file.Write(bytes)
	end := time.Now().UnixNano()

	fmt.Printf("Time used: %v\n", (end-start)/1000000)

	if err != nil {
		fmt.Println("Write error:", err)
	}
}
