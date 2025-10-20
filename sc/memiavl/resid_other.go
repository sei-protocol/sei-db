//go:build !linux

package memiavl

import "fmt"

// residentRatio stub for non-Linux; returns error so caller can continue without gating.
func residentRatio(b []byte) (float64, error) {
	return 0, fmt.Errorf("residentRatio unsupported on this platform")
}
