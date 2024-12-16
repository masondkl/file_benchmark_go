package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var MODES = []string{
	"sync", "fsync", "dsync",
	"manual_fsync", "manual_dsync",
	"direct_sync", "direct_fsync", "direct_dsync",
	"direct_manual_fsync", "direct_manual_dsync",
}

func RunOperations(fileIndex int, operations int, data []byte, file *os.File, mode string, alloc bool) {
	start := time.Now().UnixNano()
	for i := 0; i < operations; i++ {
		_, err := file.Write(data)
		if err != nil {
			fmt.Println("Error writing file: ", err)
			return
		}
	}
	end := time.Now().UnixNano()
	sec := float64(end-start) / float64(1000000000)
	opsSec := float64(operations) / sec
	allocStr := "prealloc"
	if alloc {
		allocStr = "alloc"
	}
	fmt.Printf("File(%v) Completed %v %v %v operations with data size %v bytes: (%v ops/sec over %v secs)\n", fileIndex, operations, allocStr, mode, len(data), opsSec, sec)
}

func RunDsyncOperations(fileIndex int, operations int, data []byte, file *os.File, mode string, alloc bool) {
	start := time.Now().UnixNano()
	for i := 0; i < operations; i++ {
		_, err := file.Write(data)
		if err != nil {
			fmt.Println("Error writing file: ", err)
			return
		}
		err = syscall.Fdatasync(int(file.Fd()))
		if err != nil {
			fmt.Println("Error dsyncing file: ", err)
			return
		}
	}
	end := time.Now().UnixNano()
	sec := float64(end-start) / float64(1000000000)
	opsSec := float64(operations) / sec
	allocStr := "prealloc"
	if alloc {
		allocStr = "alloc"
	}
	fmt.Printf("File(%v) Completed %v %v %v operations with data size %v bytes: (%v ops/sec over %v secs)\n", fileIndex, operations, allocStr, mode, len(data), opsSec, sec)
}

func RunFsyncOperations(fileIndex int, operations int, data []byte, file *os.File, mode string, alloc bool) {
	start := time.Now().UnixNano()
	for i := 0; i < operations; i++ {
		_, err := file.Write(data)
		if err != nil {
			fmt.Println("Error writing file: ", err)
			return
		}
		err = syscall.Fsync(int(file.Fd()))
		if err != nil {
			fmt.Println("Error fsyncing file: ", err)
			return
		}
	}
	end := time.Now().UnixNano()
	sec := float64(end-start) / float64(1000000000)
	opsSec := float64(operations) / sec
	allocStr := "prealloc"
	if alloc {
		allocStr = "alloc"
	}
	fmt.Printf("File(%v) Completed %v %v %v operations with data size %v bytes: (%v ops/sec over %v secs)\n", fileIndex, operations, allocStr, mode, len(data), opsSec, sec)
}

func Run(operations int, dataSize int, files int, mode string, alloc bool) {
	flags := 0
	if mode == "sync" {
		flags |= syscall.O_SYNC
	} else if mode == "fsync" {
		flags |= syscall.O_FSYNC
	} else if mode == "dsync" {
		flags |= syscall.O_DSYNC
	} else if mode == "direct_sync" {
		flags |= syscall.O_DIRECT | syscall.O_SYNC
	} else if mode == "direct_fsync" {
		flags |= syscall.O_DIRECT | syscall.O_FSYNC
	} else if mode == "direct_dsync" {
		flags |= syscall.O_DIRECT | syscall.O_DSYNC
	} else if mode == "direct_manual_fsync" {
		flags |= syscall.O_DIRECT
	} else if mode == "direct_manual_dsync" {
		flags |= syscall.O_DIRECT
	}
	//fmt.Printf("Starting run of %v %v operations with data size %v bytes\n", operations, mode, dataSize)
	group := sync.WaitGroup{}
	group.Add(files)
	for i := 0; i < files; i++ {
		fileIndex := i
		go func() {
			fileName := fmt.Sprintf("file-%v", fileIndex)
			_, err := os.Stat(fileName)
			if err == nil {
				err = os.Remove(fileName)
				if err != nil {
					fmt.Println("Error removing file: ", err)
					return
				}
			}
			file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|flags, 0644)
			if err != nil {
				fmt.Println("Error opening file: ", err)
				return
			}
			data := make([]byte, dataSize)
			if !alloc {
				for i := 0; i < operations; i++ {
					_, err := file.Write(data)
					if err != nil {
						fmt.Println("Error pre-allocating file: ", err)
						return
					}
				}
				if err != nil {
					fmt.Println("Error preallocating file space:", err)
					return
				}
				_, err := file.Seek(0, io.SeekStart)
				if err != nil {
					return
				}
			}
			if mode == "manual_fsync" || mode == "direct_manual_fsync" {
				RunFsyncOperations(fileIndex, operations, data, file, mode, alloc)
			} else if mode == "manual_dsync" || mode == "direct_manual_dsync" {
				RunDsyncOperations(fileIndex, operations, data, file, mode, alloc)
			} else {
				RunOperations(fileIndex, operations, data, file, mode, alloc)
			}
			err = file.Close()
			if err != nil {
				fmt.Println("Error closing file: ", err)
				return
			}
			group.Done()
		}()
	}
	group.Wait()
}

func main() {
	if len(os.Args) != 6 {
		fmt.Println("Expected arguments: (operations int) (data_size int) (files int) (mode str) (alloc bool)")
		return
	}
	operations, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Expected arguments: (operations int) (data_size int) (files int) (mode str) (alloc bool)")
		return
	}
	dataSize, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Expected arguments: (operations int) (data_size int) (files int) (mode str) (alloc bool)")
		return
	}
	files, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println("Expected arguments: (operations int) (data_size int) (files int) (mode str) (alloc bool)")
		return
	}
	mode := strings.ToLower(os.Args[4])
	alloc, err := strconv.ParseBool(os.Args[5])
	if err != nil {
		fmt.Println("Expected arguments: (operations int) (data_size int) (files int) (mode str) (alloc bool)")
		return
	}

	if mode == "all" {
		for i := 0; i < len(MODES); i++ {
			Run(operations, dataSize, files, MODES[i], alloc)
		}
	} else {
		Run(operations, dataSize, files, mode, alloc)
	}
}
