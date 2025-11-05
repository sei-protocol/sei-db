//go:build linux

package memiavl

import "syscall"

// dropFileCache hints the kernel to drop page cache for a file range
// Uses fadvise on Linux
func dropFileCache(fd int, offset, length int64) error {
	// fadvise(fd, offset, length, POSIX_FADV_DONTNEED)
	// POSIX_FADV_DONTNEED = 4 on Linux
	_, _, err := syscall.Syscall6(syscall.SYS_FADVISE64, uintptr(fd), uintptr(offset), uintptr(length), 4, 0, 0)
	if err != 0 {
		return err
	}
	return nil
}

