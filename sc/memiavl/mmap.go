package memiavl

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/mmap"
	"github.com/sei-protocol/sei-db/common/errors"
	"golang.org/x/sys/unix"
)

// MmapFile manage the resources of a mmap-ed file
type MmapFile struct {
	file *os.File
	data []byte
	// mmap handle for windows (this is used to close mmap)
	handle *[mmap.MaxMapSize]byte
}

// Open openes the file and create the mmap.
// the mmap is created with flags: PROT_READ, MAP_SHARED, MADV_RANDOM.
func NewMmap(path string) (*MmapFile, error) {
	return newMmapInternal(path, true)
}

// NewMmapNoPreload opens the file and creates mmap without prefetching hints
// Used for small/inactive trees to avoid unnecessary OS prefetching
func NewMmapNoPreload(path string) (*MmapFile, error) {
	return newMmapInternal(path, false)
}

func newMmapInternal(path string, withPrefetch bool) (*MmapFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	data, handle, err := Mmap(file)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	// Apply madvise hints based on prefetch flag
	if len(data) > 0 {
		if withPrefetch {
			// Override default MADV_RANDOM with SEQUENTIAL + WILLNEED to favor prefetching
			_ = unix.Madvise(data, unix.MADV_SEQUENTIAL)
			_ = unix.Madvise(data, unix.MADV_WILLNEED)
			fmt.Printf("[MMAP] madvise SEQUENTIAL+WILLNEED applied path=%s size=%d MB\n", path, len(data)/(1024*1024))
		} else {
			// Keep default MADV_RANDOM, no prefetching
			_ = unix.Madvise(data, unix.MADV_RANDOM)
			fmt.Printf("[MMAP] madvise RANDOM (no prefetch) path=%s size=%d MB\n", path, len(data)/(1024*1024))
		}
	}

	return &MmapFile{
		file:   file,
		data:   data,
		handle: handle,
	}, nil
}

// Close closes the file and mmap handles
func (m *MmapFile) Close() error {
	var err error
	if m.handle != nil {
		err = mmap.Munmap(m.data, m.handle)
	}
	return errors.Join(err, m.file.Close())
}

// Data returns the mmap-ed buffer
func (m *MmapFile) Data() []byte {
	return m.data
}

func Mmap(f *os.File) ([]byte, *[mmap.MaxMapSize]byte, error) {
	fi, err := f.Stat()
	if err != nil || fi.Size() == 0 {
		return nil, nil, err
	}

	return mmap.Mmap(f, int(fi.Size()))
}
