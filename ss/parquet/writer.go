package parquet

import (
    "crypto/rand"
    "encoding/hex"
    "fmt"
    "os"
    "path/filepath"
    "strings"

    "github.com/sei-protocol/sei-db/proto"
    "github.com/xitongsys/parquet-go/parquet"
    "github.com/xitongsys/parquet-go-source/local"
    "github.com/xitongsys/parquet-go/writer"
)

// OwnerExtractor determines the owner string for a KV pair.
// Implementations should be deterministic and stable across versions.
type OwnerExtractor func(storeKey string, key []byte, value []byte) string

// Writer writes change rows to partitioned Parquet files using HDFS-style layout.
type Writer struct {
    baseDir        string
    ownerExtractor OwnerExtractor
}

func NewWriter(baseDir string, extractor OwnerExtractor) (*Writer, error) {
    if baseDir == "" {
        return nil, fmt.Errorf("baseDir cannot be empty")
    }
    if extractor == nil {
        extractor = func(storeKey string, key []byte, value []byte) string { return "" }
    }
    if err := os.MkdirAll(baseDir, os.ModePerm); err != nil {
        return nil, err
    }
    return &Writer{baseDir: baseDir, ownerExtractor: extractor}, nil
}

// ChangeRow defines the schema for Parquet rows.
type ChangeRow struct {
    Version   int64  `parquet:"name=version, type=INT64"`
    StoreKey  string `parquet:"name=store_key, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
    Key       []byte `parquet:"name=key, type=BYTE_ARRAY"`
    Value     []byte `parquet:"name=value, type=BYTE_ARRAY"`
    Tombstone bool   `parquet:"name=tombstone, type=BOOLEAN"`
    Owner     string `parquet:"name=owner, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// WriteChangeSet writes the provided changes into Parquet files partitioned by version and owner.
func (w *Writer) WriteChangeSet(version int64, cs *proto.NamedChangeSet) error {
    // Keep version >= 1 in parity with MVCC backends
    if version == 0 {
        version = 1
    }
    if cs == nil || cs.Changeset.Pairs == nil || len(cs.Changeset.Pairs) == 0 {
        return nil
    }

    // Group rows by owner
    grouped := map[string][]*ChangeRow{}
    for _, kv := range cs.Changeset.Pairs {
        tomb := kv.Value == nil
        val := kv.Value
        if tomb {
            val = []byte{}
        }
        owner := strings.TrimSpace(w.ownerExtractor(cs.Name, kv.Key, kv.Value))
        row := &ChangeRow{
            Version:   version,
            StoreKey:  cs.Name,
            Key:       kv.Key,
            Value:     val,
            Tombstone: tomb,
            Owner:     owner,
        }
        grouped[owner] = append(grouped[owner], row)
    }

    for owner, rows := range grouped {
        if err := w.writePartition(version, owner, rows); err != nil {
            return err
        }
    }
    return nil
}

func (w *Writer) writePartition(version int64, owner string, rows []*ChangeRow) error {
    // Build HDFS-style partition path: version=<ver>/owner=<owner>
    partDir := filepath.Join(w.baseDir, fmt.Sprintf("version=%d", version))
    ownerValue := owner
    if ownerValue == "" {
        ownerValue = "_"
    }
    partDir = filepath.Join(partDir, fmt.Sprintf("owner=%s", ownerValue))
    if err := os.MkdirAll(partDir, os.ModePerm); err != nil {
        return err
    }

    // Unique file name
    suf := randomHex(8)
    filePath := filepath.Join(partDir, fmt.Sprintf("part-%s.parquet", suf))

    fw, err := local.NewLocalFileWriter(filePath)
    if err != nil {
        return fmt.Errorf("open parquet file: %w", err)
    }
    defer fw.Close()

    pw, err := writer.NewParquetWriter(fw, new(ChangeRow), 2)
    if err != nil {
        return fmt.Errorf("create parquet writer: %w", err)
    }
    pw.RowGroupSize = 8 * 1024 * 1024 // 8MB row groups by default
    pw.CompressionType = parquet.CompressionCodec_SNAPPY

    for _, r := range rows {
        if err := pw.Write(r); err != nil {
            _ = pw.WriteStop()
            return fmt.Errorf("write parquet row: %w", err)
        }
    }
    if err := pw.WriteStop(); err != nil {
        return fmt.Errorf("finalize parquet: %w", err)
    }
    return nil
}

func (w *Writer) Close() error { return nil }

// randomHex returns a random hex string of n bytes length.
func randomHex(n int) string {
    b := make([]byte, n)
    _, _ = rand.Read(b)
    return hex.EncodeToString(b)
}

