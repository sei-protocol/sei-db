//go:build linux
// +build linux

package memiavl

import (
	"os"
	"path/filepath"
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
		0,                   // offset
		uintptr(fi.Size()),  // length
		POSIX_FADV_DONTNEED, // advice
		0, 0,
	)
	// Ignore errors - this is just a hint to the kernel
}

// dropPageCacheRange tells the OS to drop a specific range of pages from cache
// This is more efficient than dropping entire file when doing incremental writes
// Only drops the newly written portion, avoiding expensive fadvise on entire 80GB file
func dropPageCacheRange(f *os.File, offset, end int64) {
	if f == nil || offset >= end {
		return
	}

	fd := int(f.Fd())
	const POSIX_FADV_DONTNEED = 4

	// Call fadvise64 for just the range we wrote
	length := end - offset
	_, _, _ = syscall.Syscall6(
		syscall.SYS_FADVISE64,
		uintptr(fd),
		uintptr(offset),            // start offset
		uintptr(length),            // length of range to drop
		POSIX_FADV_DONTNEED,        // advice
		0, 0,
	)
	// Ignore errors - this is just a hint to the kernel
}

// touchPageCache tells the OS to keep the file's pages in cache with high priority
// This prevents eviction by other processes (e.g., PebbleDB RPC reads)
// Critical for maintaining stable Export performance when RPC is active
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
		0,                  // offset
		uintptr(fi.Size()), // length
		POSIX_FADV_WILLNEED, // advice
		0, 0,
	)
	// Ignore errors - this is just a hint to the kernel
}

// prefetchFileRange tells the OS to asynchronously read a specific range into page cache
// This is used for streaming/incremental prefetch - only prefetch what you need next
// Much more memory-efficient than prefetching entire 80GB file
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

// dropDirectoryPageCache drops page cache for all files in a directory recursively
// This is used to evict PebbleDB cache which can grow to 20-30GB and evict our source snapshot
func dropDirectoryPageCache(dir string) {
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		// Open file and drop its cache
		if f, err := os.Open(path); err == nil {
			dropPageCache(f)
			f.Close()
		}
		return nil
	})
}
