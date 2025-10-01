package ss

import (
	"github.com/sei-protocol/sei-db/config"
	"github.com/sei-protocol/sei-db/ss/parquet"
	"github.com/sei-protocol/sei-db/ss/types"
)

const (
	// ParquetBackend represents parquet storage
	ParquetBackend BackendType = "parquet"
)

func init() {
	// Register the Parquet backend
	RegisterBackend(ParquetBackend, NewParquetBackend)
}

// NewParquetBackend creates a new Parquet-based state store
func NewParquetBackend(dir string, config config.StateStoreConfig) (types.StateStore, error) {
	return parquet.New(dir, config)
}