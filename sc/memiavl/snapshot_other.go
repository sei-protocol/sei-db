//go:build !linux

package memiavl

// dropFileCache is a no-op on non-Linux platforms
func dropFileCache(fd int, offset, length int64) error {
	// fadvise is Linux-specific, no-op on other platforms
	return nil
}

