package memiavl

import (
	"fmt"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

// residentRatio returns fraction of pages resident in the page cache for b.
// Uses mincore on Linux; on other platforms returns an unsupported error.
func residentRatio(b []byte) (float64, error) {
	if len(b) == 0 {
		return 1, nil
	}
	if runtime.GOOS != "linux" {
		return 0, fmt.Errorf("residentRatio unsupported on %s", runtime.GOOS)
	}

	pageSize := unix.Getpagesize()
	numPages := (len(b) + pageSize - 1) / pageSize
	if numPages == 0 {
		return 1, nil
	}
	vec := make([]byte, numPages)

	addr := uintptr(unsafe.Pointer(&b[0]))
	length := uintptr(len(b))
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, addr, length, uintptr(unsafe.Pointer(&vec[0])))
	if errno != 0 {
		return 0, errno
	}

	present := 0
	for _, v := range vec {
		if v&1 == 1 {
			present++
		}
	}
	return float64(present) / float64(len(vec)), nil
}
