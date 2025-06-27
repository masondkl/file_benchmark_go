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

func Time(fileIndex int, operations int, mode string, alloc bool, operation string, dataSize int, block func()) {
	start := time.Now().UnixNano()
	block()
	end := time.Now().UnixNano()
	sec := float64(end-start) / float64(1000000000)
	opsSec := float64(operations) / sec
	allocStr := "prealloc"
	if alloc {
		allocStr = "alloc"
	}
	fmt.Printf(
		"File(%v) Completed %v %v %v %v operations with data size %v bytes: (%v ops/sec over %v secs)\n",
		fileIndex, operations, allocStr, mode, operation, dataSize, opsSec, sec,
	)
}

func RunReadOperations(fileIndex int, operations int, mode string, alloc bool, operation string, data []byte, file *os.File) {
	Time(fileIndex, operations, mode, alloc, operation, len(data), func() {
		for i := 0; i < operations; i++ {
			n, err := file.Read(data)
			if n != len(data) || err != nil {
				fmt.Println("Error reading file: ", err)
				return
			}
		}
	})
}

func RunDsyncReadOperations(fileIndex int, operations int, mode string, alloc bool, operation string, data []byte, file *os.File) {
	Time(fileIndex, operations, mode, alloc, operation, len(data), func() {
		for i := 0; i < operations; i++ {
			err := syscall.Fdatasync(int(file.Fd()))
			if err != nil {
				fmt.Println("Error dsyncing file: ", err)
				return
			}
			n, err := file.Read(data)
			if n != len(data) || err != nil {
				fmt.Println("Error reading file: ", err)
				return
			}
		}
	})
}

func RunFsyncReadOperations(fileIndex int, operations int, mode string, alloc bool, operation string, data []byte, file *os.File) {
	Time(fileIndex, operations, mode, alloc, operation, len(data), func() {
		for i := 0; i < operations; i++ {
			err := syscall.Fsync(int(file.Fd()))
			if err != nil {
				fmt.Println("Error fsyncing file: ", err)
				return
			}
			n, err := file.Read(data)
			if n != len(data) || err != nil {
				fmt.Println("Error reading file: ", err)
				return
			}
		}
	})
}

func RunWriteOperations(fileIndex int, operations int, mode string, alloc bool, operation string, data []byte, file *os.File) {
	Time(fileIndex, operations, mode, alloc, operation, len(data), func() {
		for i := 0; i < operations; i++ {
			n, err := file.Write(data)
			if n != len(data) || err != nil {
				fmt.Println("Error writing file: ", err)
				return
			}
		}
	})
}

func RunDsyncWriteOperations(fileIndex int, operations int, mode string, alloc bool, operation string, data []byte, file *os.File) {
	Time(fileIndex, operations, mode, alloc, operation, len(data), func() {
		for i := 0; i < operations; i++ {
			n, err := file.Write(data)
			if n != len(data) || err != nil {
				fmt.Println("Error writing file: ", err)
				return
			}
			err = syscall.Fdatasync(int(file.Fd()))
			if err != nil {
				fmt.Println("Error dsyncing file: ", err)
				return
			}
		}
	})
}

func RunFsyncWriteOperations(fileIndex int, operations int, mode string, alloc bool, operation string, data []byte, file *os.File) {
	Time(fileIndex, operations, mode, alloc, operation, len(data), func() {
		for i := 0; i < operations; i++ {
			n, err := file.Write(data)
			if n != len(data) || err != nil {
				fmt.Println("Error writing file: ", err)
				return
			}
			err = syscall.Fsync(int(file.Fd()))
			if err != nil {
				fmt.Println("Error fsyncing file: ", err)
				return
			}
		}
	})
}

func Run(operations int, dataSize int, files int, mode string, operation string, alloc bool) {
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
	group := sync.WaitGroup{}
	group.Add(files)

	start := time.Now().UnixNano()
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
			if !alloc || operation == "read" {
				data := make([]byte, dataSize*operations)
				n, err := file.Write(data)
				if n != len(data) || err != nil {
					fmt.Println("Error pre-allocating file: ", err)
					return
				}
				_, err = file.Seek(0, io.SeekStart)
				if err != nil {
					fmt.Println("Error seeking file: ", err)
					return
				}
				err = syscall.Fsync(int(file.Fd()))
				if err != nil {
					fmt.Println("Error fsyncing file: ", err)
					return
				}
			}

			data := make([]byte, dataSize)
			if mode == "manual_fsync" || mode == "direct_manual_fsync" {
				if operation == "write" {
					RunFsyncWriteOperations(fileIndex, operations, mode, alloc, operation, data, file)
				} else {
					RunFsyncReadOperations(fileIndex, operations, mode, alloc, operation, data, file)
				}
			} else if mode == "manual_dsync" || mode == "direct_manual_dsync" {
				if operation == "write" {
					RunDsyncWriteOperations(fileIndex, operations, mode, alloc, operation, data, file)
				} else {
					RunDsyncReadOperations(fileIndex, operations, mode, alloc, operation, data, file)
				}
			} else {
				if operation == "write" {
					RunWriteOperations(fileIndex, operations, mode, alloc, operation, data, file)
				} else {
					RunReadOperations(fileIndex, operations, mode, alloc, operation, data, file)
				}
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
	end := time.Now().UnixNano()
	sec := float64(end-start) / float64(1000000000)
	opsSec := float64(operations*files) / sec
	allocStr := "prealloc"
	if alloc {
		allocStr = "alloc"
	}
	fmt.Printf(
		"Completed %v %v %v %v operations with data size %v bytes: (%v ops/sec over %v secs)\n",
		operations*files, allocStr, mode, operation, dataSize, opsSec, sec,
	)
}

func PrintArguments() {
	fmt.Println("Expected arguments: (operations int) (data_size int) (files int) (mode string) (operation string) (alloc bool)")
	fmt.Println("Modes: all, ", strings.Join(MODES, ", "))
	fmt.Println("Operations: read, write")
}

func main() {
	if len(os.Args) != 7 {
		fmt.Println("1")
		PrintArguments()

		return
	}
	operations, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("2")
		PrintArguments()
		return
	}
	dataSize, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("3")
		PrintArguments()
		return
	}
	files, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println("4")
		PrintArguments()
		return
	}
	mode := strings.ToLower(os.Args[4])
	success := false
	for i := 0; i < len(MODES); i++ {
		if MODES[i] == mode {
			success = true
		}
	}
	if mode != "all" && !success {
		fmt.Println("5")
		PrintArguments()
		return
	}
	operation := strings.ToLower(os.Args[5])
	if operation != "write" && operation != "read" {
		fmt.Println("6")
		PrintArguments()
		return
	}
	alloc, err := strconv.ParseBool(os.Args[6])
	if err != nil {
		fmt.Println("7")
		PrintArguments()
		return
	}
	if mode == "all" {
		for i := 0; i < len(MODES); i++ {
			Run(operations, dataSize, files, MODES[i], operation, alloc)
		}
	} else {
		Run(operations, dataSize, files, mode, operation, alloc)
	}
}
