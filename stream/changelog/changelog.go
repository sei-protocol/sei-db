package changelog

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/sei-protocol/sei-db/common/logger"
	"github.com/sei-protocol/sei-db/common/utils"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/stream"
	"github.com/tidwall/wal"
)

var _ stream.Stream[proto.ChangelogEntry] = (*Stream)(nil)

type Stream struct {
	log            *wal.Log
	config         Config
	logger         logger.Logger
	writeChannel   chan *Message
	writeErrSignal chan error
}

type Message struct {
	Index uint64
	Data  *proto.ChangelogEntry
}

type Config struct {
	DisableFsync    bool
	ZeroCopy        bool
	WriteBufferSize int
}

// NewStream creates a new changelog stream that persist the changesets in the log
func NewStream(logger logger.Logger, dir string, config Config) (*Stream, error) {
	log, err := open(dir, &wal.Options{
		NoSync: config.DisableFsync,
		NoCopy: config.ZeroCopy,
	})
	if err != nil {
		return nil, err
	}
	return &Stream{
		log:    log,
		config: config,
		logger: logger,
	}, nil

}

// Write will write a new entry to the log at given index.
// Whether the writes is in blocking or async manner depends on the buffer size.
func (stream *Stream) Write(offset uint64, entry proto.ChangelogEntry) error {
	channelBufferSize := stream.config.WriteBufferSize
	if channelBufferSize > 0 {
		if stream.writeChannel == nil {
			stream.logger.Info(fmt.Sprintf("async write is enabled with buffer size %d", channelBufferSize))
			stream.writeChannel = make(chan *Message, channelBufferSize)
			stream.writeErrSignal = make(chan error)
			go stream.startWriteGoroutine()
		}
		// async write
		stream.writeChannel <- &Message{Index: offset, Data: &entry}
	} else {
		// synchronous write
		bz, err := entry.Marshal()
		if err != nil {
			return err
		}
		if err := stream.log.Write(offset, bz); err != nil {
			return err
		}
	}
	return nil
}

// startWriteGoroutine will start a goroutine to write entries to the log.
// This should only be called on initialization if async write is enabled
func (stream *Stream) startWriteGoroutine() {
	batch := wal.Batch{}
	defer close(stream.writeErrSignal)
	for {
		entries := channelBatchRecv(stream.writeChannel)
		if len(entries) == 0 {
			// channel is closed
			break
		}

		for _, entry := range entries {
			bz, err := entry.Data.Marshal()
			if err != nil {
				stream.writeErrSignal <- err
				return
			}
			batch.Write(entry.Index, bz)
		}

		if err := stream.log.WriteBatch(&batch); err != nil {
			stream.writeErrSignal <- err
			return
		}
		batch.Clear()
	}
}

// TruncateAfter will remove all entries that are after the provided `index`.
// In other words the entry at `index` becomes the last entry in the log.
func (stream *Stream) TruncateAfter(index uint64) error {
	return stream.log.TruncateBack(index)
}

// TruncateBefore will remove all entries that are before the provided `index`.
// In other words the entry at `index` becomes the first entry in the log.
func (stream *Stream) TruncateBefore(index uint64) error {
	return stream.log.TruncateFront(index)
}

// CheckError check if there's any failed async writes or not
func (stream *Stream) CheckError() error {
	select {
	case err := <-stream.writeErrSignal:
		// async wal writing failed, we need to abort the state machine
		return fmt.Errorf("async wal writing goroutine quit unexpectedly: %w", err)
	default:
	}
	return nil
}

// Flush will block and wait for async writes to complete
func (stream *Stream) Flush() error {
	if stream.writeChannel == nil {
		return nil
	}
	close(stream.writeChannel)
	err := <-stream.writeErrSignal
	stream.writeChannel = nil
	stream.writeErrSignal = nil
	return err
}

// LastOffset returns the last written offset/index of the log
func (stream *Stream) LastOffset() (index uint64, err error) {
	return stream.log.LastIndex()
}

// ReadAt will read the log entry at the provided index
func (stream *Stream) ReadAt(index uint64) (*proto.ChangelogEntry, error) {
	var entry = &proto.ChangelogEntry{}
	bz, err := stream.log.Read(index)
	if err != nil {
		return entry, fmt.Errorf("read log failed, %w", err)
	}
	if err := entry.Unmarshal(bz); err != nil {
		return entry, fmt.Errorf("unmarshal rlog failed, %w", err)
	}
	return entry, nil
}

// Replay will read the replay log and process each log entry with the provided function
func (stream *Stream) Replay(start uint64, end uint64, processFn func(index uint64, entry proto.ChangelogEntry) error) error {
	for i := start; i <= end; i++ {
		var entry proto.ChangelogEntry
		bz, err := stream.log.Read(i)
		if err != nil {
			return fmt.Errorf("read log failed, %w", err)
		}
		if err := entry.Unmarshal(bz); err != nil {
			return fmt.Errorf("unmarshal rlog failed, %w", err)
		}
		err = processFn(i, entry)
		if err != nil {
			return err
		}
	}
	return nil
}

func (stream *Stream) Close() error {
	errWriter := stream.Flush()
	errClose := stream.log.Close()
	return utils.Join(errWriter, errClose)
}

// open opens the replay log, try to truncate the corrupted tail if there's any
func open(dir string, opts *wal.Options) (*wal.Log, error) {
	rlog, err := wal.Open(dir, opts)
	if errors.Is(err, wal.ErrCorrupt) {
		// try to truncate corrupted tail
		var fis []os.DirEntry
		fis, err = os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("read wal dir fail: %w", err)
		}
		var lastSeg string
		for _, fi := range fis {
			if fi.IsDir() || len(fi.Name()) < 20 {
				continue
			}
			lastSeg = fi.Name()
		}

		if len(lastSeg) == 0 {
			return nil, err
		}
		if err = truncateCorruptedTail(filepath.Join(dir, lastSeg), opts.LogFormat); err != nil {
			return nil, fmt.Errorf("truncate corrupted tail fail: %w", err)
		}

		// try again
		return wal.Open(dir, opts)
	}
	return rlog, err
}
