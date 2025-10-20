//go:build linux

package memiavl

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

// residentRatio returns fraction of pages resident in the page cache for b (Linux).
func residentRatio(b []byte) (float64, error) {
	if len(b) == 0 {
		return 1, nil
	}

	pageSize := unix.Getpagesize()
	// Number of pages covering the slice
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
