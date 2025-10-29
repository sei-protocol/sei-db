//go:build linux
// +build linux

package memiavl

import (
	"os"
	"syscall"
)

// dropPageCache tells the OS to drop the file's pages from page cache
// This prevents written data from evicting source snapshot pages during rewrite
// Critical for maintaining read-side cache hit rate in the later stages
func dropPageCache(f *os.File) {
	if f == nil {
		return
	}
	
	// Use posix_fadvise(POSIX_FADV_DONTNEED) to drop pages from cache
	// POSIX_FADV_DONTNEED = 4 on Linux
	// syscall: fadvise64(fd, offset, len, advice)
	fd := int(f.Fd())
	const POSIX_FADV_DONTNEED = 4
	
	// Get file size
	fi, err := f.Stat()
	if err != nil {
		return
	}
	
	// Call fadvise64 syscall
	_, _, _ = syscall.Syscall6(
		syscall.SYS_FADVISE64,
		uintptr(fd),
		0,                      // offset
		uintptr(fi.Size()),     // length
		POSIX_FADV_DONTNEED,    // advice
		0, 0,
	)
	// Ignore errors - this is just a hint to the kernel
}

