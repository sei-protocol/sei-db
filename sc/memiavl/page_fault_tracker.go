package memiavl

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// PageFaultStats tracks major and minor page faults
type PageFaultStats struct {
	MinorFaults uint64
	MajorFaults uint64
}

// GetPageFaultStats reads current process page fault statistics from /proc/self/stat
func GetPageFaultStats() (*PageFaultStats, error) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return nil, err
	}

	fields := strings.Fields(string(data))
	if len(fields) < 12 {
		return nil, fmt.Errorf("unexpected stat format")
	}

	// Field 10: minflt (minor faults)
	// Field 12: majflt (major faults)
	minorFaults, _ := strconv.ParseUint(fields[9], 10, 64)
	majorFaults, _ := strconv.ParseUint(fields[11], 10, 64)

	return &PageFaultStats{
		MinorFaults: minorFaults,
		MajorFaults: majorFaults,
	}, nil
}

func (s *PageFaultStats) Delta(before *PageFaultStats) *PageFaultStats {
	return &PageFaultStats{
		MinorFaults: s.MinorFaults - before.MinorFaults,
		MajorFaults: s.MajorFaults - before.MajorFaults,
	}
}

func (s *PageFaultStats) String() string {
	return fmt.Sprintf("minor=%d major=%d", s.MinorFaults, s.MajorFaults)
}
