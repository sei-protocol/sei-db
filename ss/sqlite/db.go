package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"

	"github.com/cosmos/cosmos-sdk/store/types"
	"github.com/sei-protocol/sei-db/common/utils"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/ss"
)

const (
	driverName       = "sqlite"
	dbName           = "ss.db"
	reservedStoreKey = "_RESERVED_"
	keyLatestHeight  = "latest_height"

	latestVersionStmt = `
	INSERT INTO state_storage(store_key, key, value, version)
    VALUES(?, ?, ?, ?)
  ON CONFLICT(store_key, key, version) DO UPDATE SET
    value = ?;
	`
	upsertStmt = `
	INSERT INTO state_storage(store_key, key, value, version)
    VALUES(?, ?, ?, ?)
  ON CONFLICT(store_key, key, version) DO UPDATE SET
    value = ?;
	`
	delStmt = `
	UPDATE state_storage SET tombstone = ?
	WHERE id = (
		SELECT id FROM state_storage WHERE store_key = ? AND key = ? AND version <= ? ORDER BY version DESC LIMIT 1
	) AND tombstone = 0;
	`
)

var _ ss.StateStore = (*Database)(nil)

type Database struct {
	storage *sql.DB
}

func New(dataDir string) (*Database, error) {
	db, err := sql.Open(driverName, filepath.Join(dataDir, dbName))
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite DB: %w", err)
	}

	stmt := `
	CREATE TABLE IF NOT EXISTS state_storage (
		id integer not null primary key, 
		store_key varchar not null,
		key varchar not null,
		value varchar not null,
		version integer not null,
		tombstone integer default 0,
		unique (store_key, key, version)
	);

	CREATE UNIQUE INDEX IF NOT EXISTS idx_store_key_version ON state_storage (store_key, key, version);
	`
	_, err = db.Exec(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to exec SQL statement: %w", err)
	}

	return &Database{
		storage: db,
	}, nil
}

func (db *Database) Close() error {
	err := db.storage.Close()
	db.storage = nil
	return err
}

func (db *Database) GetLatestVersion() (int64, error) {
	stmt, err := db.storage.Prepare("SELECT value FROM state_storage WHERE store_key = ? AND key = ?")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}

	defer stmt.Close()

	var latestHeight int64
	if err := stmt.QueryRow(reservedStoreKey, keyLatestHeight).Scan(&latestHeight); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// in case of a fresh database
			return 0, nil
		}

		return 0, fmt.Errorf("failed to query row: %w", err)
	}

	return latestHeight, nil
}

func (db *Database) SetLatestVersion(version int64) error {
	_, err := db.storage.Exec(latestVersionStmt, reservedStoreKey, keyLatestHeight, version, 0, version)
	if err != nil {
		return fmt.Errorf("failed to exec SQL statement: %w", err)
	}

	return nil
}

func (db *Database) Has(storeKey string, version int64, key []byte) (bool, error) {
	val, err := db.Get(storeKey, version, key)
	if err != nil {
		return false, err
	}

	return val != nil, nil
}

func (db *Database) Get(storeKey string, targetVersion int64, key []byte) ([]byte, error) {
	stmt, err := db.storage.Prepare(`
	SELECT value, tombstone FROM state_storage
	WHERE store_key = ? AND key = ? AND version <= ?
	ORDER BY version DESC LIMIT 1;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}

	defer stmt.Close()

	var (
		value []byte
		tomb  int64
	)
	if err := stmt.QueryRow(storeKey, key, targetVersion).Scan(&value, &tomb); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to query row: %w", err)
	}

	// A tombstone of zero or a target version that is less than the tombstone
	// version means the key is not deleted at the target version.
	if tomb == 0 || targetVersion < tomb {
		return value, nil
	}

	// the value is considered deleted
	return nil, nil
}

func (db *Database) ApplyChangeset(version int64, cs *proto.NamedChangeSet) error {
	b, err := NewBatch(db.storage, version)
	if err != nil {
		return err
	}

	for _, kvPair := range cs.Changeset.Pairs {
		if kvPair.Value == nil {
			if err := b.Delete(cs.Name, kvPair.Key); err != nil {
				return err
			}
		} else {
			if err := b.Set(cs.Name, kvPair.Key, kvPair.Value); err != nil {
				return err
			}
		}
	}

	return b.Write()
}

func (db *Database) Prune(version int64) error {
	stmt := "DELETE FROM state_storage WHERE version <= ? AND store_key != ?;"

	_, err := db.storage.Exec(stmt, version, reservedStoreKey)
	if err != nil {
		return fmt.Errorf("failed to exec SQL statement: %w", err)
	}

	return nil
}

func (db *Database) Iterator(storeKey string, version int64, start, end []byte) (types.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, utils.ErrKeyEmpty
	}

	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, utils.ErrStartAfterEnd
	}

	return newIterator(db.storage, storeKey, version, start, end, false)
}

func (db *Database) ReverseIterator(storeKey string, version int64, start, end []byte) (types.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, utils.ErrKeyEmpty
	}

	if start != nil && end != nil && bytes.Compare(start, end) > 0 {
		return nil, utils.ErrStartAfterEnd
	}

	return newIterator(db.storage, storeKey, version, start, end, true)
}

func (db *Database) Import(version int64, ch <-chan ss.ImportEntry) error {
	panic("Not Implemented")
}

func (db *Database) PrintRowsDebug() {
	stmt, err := db.storage.Prepare("SELECT store_key, key, value, version, tombstone FROM state_storage")
	if err != nil {
		panic(fmt.Errorf("failed to prepare SQL statement: %w", err))
	}

	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		panic(fmt.Errorf("failed to execute SQL query: %w", err))
	}

	var sb strings.Builder
	for rows.Next() {
		var (
			storeKey string
			key      []byte
			value    []byte
			version  int64
			tomb     int64
		)
		if err := rows.Scan(&storeKey, &key, &value, &version, &tomb); err != nil {
			panic(fmt.Sprintf("failed to scan row: %s", err))
		}

		sb.WriteString(fmt.Sprintf("STORE_KEY: %s, KEY: %s, VALUE: %s, VERSION: %d, TOMBSTONE: %d\n", storeKey, key, value, version, tomb))
	}
	if err := rows.Err(); err != nil {
		panic(fmt.Errorf("received unexpected error: %w", err))
	}

	fmt.Println(strings.TrimSpace(sb.String()))
}