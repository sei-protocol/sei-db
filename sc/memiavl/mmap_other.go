//go:build !linux
// +build !linux

package memiavl

import "os"

// prefetchFileRange is a no-op on non-Linux platforms
func prefetchFileRange(f *os.File, offset, end int64) {
	// No-op on non-Linux platforms
	// macOS/Windows don't have equivalent functionality
}

// touchPageCache is a no-op on non-Linux platforms
func touchPageCache(f *os.File) {
	// No-op on non-Linux platforms
	// macOS/Windows don't have equivalent functionality
}
