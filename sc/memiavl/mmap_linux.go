//go:build linux
// +build linux

package memiavl

import (
	"os"
	"syscall"
)

// prefetchFileRange tells the OS to asynchronously read a specific range into page cache
// This is used for streaming/incremental prefetch during cold start
// Much more memory-efficient than prefetching entire 80GB file at once
func prefetchFileRange(f *os.File, offset, end int64) {
	if f == nil || offset >= end {
		return
	}

	fd := int(f.Fd())
	const POSIX_FADV_WILLNEED = 3

	length := end - offset
	_, _, _ = syscall.Syscall6(
		syscall.SYS_FADVISE64,
		uintptr(fd),
		uintptr(offset),     // start offset
		uintptr(length),     // length to prefetch
		POSIX_FADV_WILLNEED, // advice - async read into cache
		0, 0,
	)
	// Ignore errors - this is just a hint to the kernel
}

// touchPageCache tells the OS to keep the file's pages in cache with high priority
// This prevents eviction by other processes (e.g., PebbleDB RPC reads)
// Used by KeepInCache to periodically refresh cache priority
func touchPageCache(f *os.File) {
	if f == nil {
		return
	}

	// Use posix_fadvise(POSIX_FADV_WILLNEED) to boost cache priority
	// POSIX_FADV_WILLNEED = 3 on Linux
	// This hints kernel to keep pages in cache and not evict them
	fd := int(f.Fd())
	const POSIX_FADV_WILLNEED = 3

	// Get file size
	fi, err := f.Stat()
	if err != nil {
		return
	}

	// Call fadvise64 syscall
	// This is lightweight - just updates page flags, doesn't actually read data
	_, _, _ = syscall.Syscall6(
		syscall.SYS_FADVISE64,
		uintptr(fd),
		0,                   // offset
		uintptr(fi.Size()),  // length
		POSIX_FADV_WILLNEED, // advice
		0, 0,
	)
	// Ignore errors - this is just a hint to the kernel
}
