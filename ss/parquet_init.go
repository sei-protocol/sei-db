package ss

import (
    "path/filepath"

    "github.com/sei-protocol/sei-db/common/utils"
    "github.com/sei-protocol/sei-db/config"
    parquetbackend "github.com/sei-protocol/sei-db/ss/parquet"
    "github.com/sei-protocol/sei-db/ss/pebbledb"
    "github.com/sei-protocol/sei-db/ss/types"
)

func init() {
    initializer := func(dir string, configs config.StateStoreConfig) (types.StateStore, error) {
        // Underlying KV store (Pebble)
        kvDir := utils.GetStateStorePath(dir, "pebbledb")
        parquetDir := utils.GetStateStorePath(dir, "parquet_data")
        if configs.DBDirectory != "" {
            kvDir = filepath.Join(configs.DBDirectory, "pebbledb")
            parquetDir = filepath.Join(configs.DBDirectory, "parquet_data")
        }

        kv, err := pebbledb.New(kvDir, configs)
        if err != nil {
            return nil, err
        }

        writer, err := parquetbackend.NewWriter(parquetDir, nil)
        if err != nil {
            _ = kv.Close()
            return nil, err
        }

        return parquetbackend.NewParquetBackend(kv, writer)
    }
    RegisterBackend(ParquetBackend, initializer)
}

