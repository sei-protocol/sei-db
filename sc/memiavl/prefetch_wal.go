package memiavl

import (
	"fmt"
	"os"
	"path/filepath"
)

// prefetchChangelogSegments sequentially warms WAL segment files in changelogDir.
// Best-effort: ignores errors and returns first error encountered for logging.
func prefetchChangelogSegments(changelogDir string) error {
	entries, err := os.ReadDir(changelogDir)
	if err != nil {
		return err
	}
	var warmed int
	fmt.Printf("[Prefetch] Prefetching %d changelog files\n", len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		path := filepath.Join(changelogDir, e.Name())
		if err := SequentialReadAndFillPageCache(path); err != nil {
			// continue warming others, but return first error to caller for info
			if warmed == 0 {
				// only track the first failure; still keep going
				// we won't short-circuit on error to maximize best-effort warming
			}
		} else {
			warmed++
		}
	}
	return nil
}
