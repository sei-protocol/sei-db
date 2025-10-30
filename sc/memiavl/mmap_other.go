//go:build !linux
// +build !linux

package memiavl

import "os"

// dropPageCache is a no-op on non-Linux platforms
func dropPageCache(f *os.File) {
	// No-op on non-Linux platforms
	// macOS/Windows don't have equivalent functionality
}

// touchPageCache is a no-op on non-Linux platforms
func touchPageCache(f *os.File) {
	// No-op on non-Linux platforms
	// macOS/Windows don't have equivalent functionality
}

