package ss_test

import (
	"testing"

	"github.com/sei-protocol/sei-db/config"
	"github.com/sei-protocol/sei-db/ss"
	"github.com/sei-protocol/sei-db/ss/parquet"
	sstest "github.com/sei-protocol/sei-db/ss/test"
	"github.com/sei-protocol/sei-db/ss/types"
	"github.com/stretchr/testify/suite"
)

func TestParquetStorageTestSuite(t *testing.T) {
	suite.Run(t, &sstest.StorageTestSuite{
		NewDB: func(dir string, cfg config.StateStoreConfig) (types.StateStore, error) {
			return parquet.New(dir, cfg)
		},
		EmptyBatchSize: 0,
		SkipTests:      []string{}, // Add any tests to skip if needed
		Config: config.StateStoreConfig{
			Backend:              string(ss.ParquetBackend),
			KeepRecent:           0,
			PruneIntervalSeconds: 0,
		},
	})
}