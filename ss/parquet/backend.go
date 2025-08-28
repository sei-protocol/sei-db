package parquet

import (
    "fmt"

    errorutils "github.com/sei-protocol/sei-db/common/errors"
    "github.com/sei-protocol/sei-db/proto"
    "github.com/sei-protocol/sei-db/ss/types"
)

// ParquetBackend is a wrapper over an underlying StateStore that also writes
// changes to Parquet files partitioned by version and owner for fast analytics/debugging.
type ParquetBackend struct {
    underlying types.StateStore
    writer     *Writer
}

func NewParquetBackend(underlying types.StateStore, writer *Writer) (*ParquetBackend, error) {
    if underlying == nil {
        return nil, fmt.Errorf("underlying store cannot be nil")
    }
    if writer == nil {
        return nil, fmt.Errorf("parquet writer cannot be nil")
    }
    return &ParquetBackend{underlying: underlying, writer: writer}, nil
}

func (p *ParquetBackend) Close() error {
    var err error
    if p.writer != nil {
        err = p.writer.Close()
    }
    if p.underlying != nil {
        if errU := p.underlying.Close(); err == nil {
            err = errU
        } else if errU != nil {
            err = errorutils.Join(err, errU)
        }
    }
    return err
}

// Delegated read APIs
func (p *ParquetBackend) Get(storeKey string, version int64, key []byte) ([]byte, error) {
    return p.underlying.Get(storeKey, version, key)
}
func (p *ParquetBackend) Has(storeKey string, version int64, key []byte) (bool, error) {
    return p.underlying.Has(storeKey, version, key)
}
func (p *ParquetBackend) Iterator(storeKey string, version int64, start, end []byte) (types.DBIterator, error) {
    return p.underlying.Iterator(storeKey, version, start, end)
}
func (p *ParquetBackend) ReverseIterator(storeKey string, version int64, start, end []byte) (types.DBIterator, error) {
    return p.underlying.ReverseIterator(storeKey, version, start, end)
}
func (p *ParquetBackend) RawIterate(storeKey string, fn func([]byte, []byte, int64) bool) (bool, error) {
    return p.underlying.RawIterate(storeKey, fn)
}
func (p *ParquetBackend) GetLatestVersion() (int64, error) { return p.underlying.GetLatestVersion() }
func (p *ParquetBackend) SetLatestVersion(version int64) error { return p.underlying.SetLatestVersion(version) }
func (p *ParquetBackend) GetEarliestVersion() (int64, error) { return p.underlying.GetEarliestVersion() }
func (p *ParquetBackend) SetEarliestVersion(version int64, ignoreVersion bool) error {
    return p.underlying.SetEarliestVersion(version, ignoreVersion)
}
func (p *ParquetBackend) GetLatestMigratedKey() ([]byte, error) { return p.underlying.GetLatestMigratedKey() }
func (p *ParquetBackend) SetLatestMigratedKey(key []byte) error { return p.underlying.SetLatestMigratedKey(key) }
func (p *ParquetBackend) GetLatestMigratedModule() (string, error) {
    return p.underlying.GetLatestMigratedModule()
}
func (p *ParquetBackend) SetLatestMigratedModule(module string) error {
    return p.underlying.SetLatestMigratedModule(module)
}
func (p *ParquetBackend) WriteBlockRangeHash(storeKey string, beginBlockRange, endBlockRange int64, hash []byte) error {
    return p.underlying.WriteBlockRangeHash(storeKey, beginBlockRange, endBlockRange, hash)
}
func (p *ParquetBackend) DeleteKeysAtVersion(module string, version int64) error {
    return p.underlying.DeleteKeysAtVersion(module, version)
}

// Mutating APIs with Parquet side-writes
func (p *ParquetBackend) ApplyChangeset(version int64, cs *proto.NamedChangeSet) error {
    if err := p.underlying.ApplyChangeset(version, cs); err != nil {
        return err
    }
    // Best-effort Parquet write after underlying commit
    if err := p.writer.WriteChangeSet(version, cs); err != nil {
        // Do not fail core path; surface error for logging upstream
        return errorutils.Join(nil, fmt.Errorf("parquet write failed: %w", err))
    }
    return nil
}

func (p *ParquetBackend) ApplyChangesetAsync(version int64, changesets []*proto.NamedChangeSet) error {
    // Fire parquet writes immediately as best-effort
    for _, cs := range changesets {
        _ = p.writer.WriteChangeSet(version, cs)
    }
    return p.underlying.ApplyChangesetAsync(version, changesets)
}

func (p *ParquetBackend) Import(version int64, ch <-chan types.SnapshotNode) error {
    // Delegate import to underlying. Parquet writes for full import are optional; skip for speed.
    return p.underlying.Import(version, ch)
}

func (p *ParquetBackend) RawImport(ch <-chan types.RawSnapshotNode) error {
    // Delegate raw import to underlying.
    return p.underlying.RawImport(ch)
}

func (p *ParquetBackend) Prune(version int64) error { return p.underlying.Prune(version) }

