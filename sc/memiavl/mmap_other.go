//go:build !linux
// +build !linux

package memiavl

import "os"

// dropPageCache is a no-op on non-Linux platforms
func dropPageCache(f *os.File) {
	// No-op on non-Linux platforms
	// macOS/Windows don't have equivalent functionality
}

// dropPageCacheRange is a no-op on non-Linux platforms
func dropPageCacheRange(f *os.File, offset, end int64) {
	// No-op on non-Linux platforms
	// macOS/Windows don't have equivalent functionality
}

// prefetchFileRange is a no-op on non-Linux platforms
func prefetchFileRange(f *os.File, offset, end int64) {
	// No-op on non-Linux platforms
	// macOS/Windows don't have equivalent functionality
}

// dropDirectoryPageCache is a no-op on non-Linux platforms
func dropDirectoryPageCache(dir string) {
	// No-op on non-Linux platforms
	// macOS/Windows don't have equivalent functionality
}

// touchPageCache is a no-op on non-Linux platforms
func touchPageCache(f *os.File) {
	// No-op on non-Linux platforms
	// macOS/Windows don't have equivalent functionality
}

