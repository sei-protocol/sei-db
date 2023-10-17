package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/exp/slices"
	_ "modernc.org/sqlite"
)

type batchAction int

const (
	driverName                  = "sqlite"
	dbName                      = "ss.db"
	keyLatestHeight             = "latest_height"
	batchActionSet  batchAction = 0
	batchActionDel  batchAction = 1

	latestVersionStmt = `
	INSERT INTO state_storage(key, value, version)
    VALUES(?, ?, ?)
  ON CONFLICT(key, version) DO UPDATE SET
    value = ?;
	`
	upsertStmt = `
	INSERT INTO state_storage(key, value, version)
    VALUES(?, ?, ?)
  ON CONFLICT(key, version) DO UPDATE SET
    value = ?;
	`
	delStmt = `
	UPDATE state_storage SET tombstone = ?
	WHERE id = (
		SELECT id FROM state_storage WHERE key = ? AND version <= ? ORDER BY version DESC LIMIT 1
	) AND tombstone = 0;
	`
)

type SqliteBackend struct{}

// NOTE: Adapted from cosmos-sdk store-v2

type Database struct {
	storage *sql.DB
}

type batchOp struct {
	action     batchAction
	key, value []byte
}

type Batch struct {
	tx      *sql.Tx
	ops     []batchOp
	size    int
	version uint64
}

func New(dataDir string) (*Database, error) {
	db, err := sql.Open(driverName, filepath.Join(dataDir, dbName))
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite DB: %w", err)
	}

	stmt := `
	CREATE TABLE IF NOT EXISTS state_storage (
		id integer not null primary key, 
		key varchar not null,
		value varchar not null,
		version integer unsigned not null,
		tombstone integer unsigned default 0,
		unique (key, version)
	);

	CREATE UNIQUE INDEX IF NOT EXISTS idx_key_version ON state_storage (key, version);
	`
	_, err = db.Exec(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to exec SQL statement: %w", err)
	}

	return &Database{
		storage: db,
	}, nil
}

func (db *Database) Get(targetVersion uint64, key []byte) ([]byte, error) {
	stmt, err := db.storage.Prepare(`
	SELECT value, tombstone FROM state_storage
	WHERE key = ? AND version <= ?
	ORDER BY version DESC LIMIT 1;
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}

	defer stmt.Close()

	var (
		value []byte
		tomb  uint64
	)
	if err := stmt.QueryRow(key, targetVersion).Scan(&value, &tomb); err != nil {
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

func (db *Database) Close() error {
	err := db.storage.Close()
	db.storage = nil
	return err
}

func NewBatch(storage *sql.DB, version uint64) (*Batch, error) {
	tx, err := storage.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to create SQL transaction: %w", err)
	}

	return &Batch{
		tx:      tx,
		ops:     make([]batchOp, 0),
		version: version,
	}, nil
}

func (b *Batch) Set(key, value []byte) error {
	b.size += len(key) + len(value)
	b.ops = append(b.ops, batchOp{action: batchActionSet, key: key, value: value})
	return nil
}

func (b *Batch) Write() error {
	_, err := b.tx.Exec(latestVersionStmt, keyLatestHeight, b.version, 0, b.version)
	if err != nil {
		return fmt.Errorf("failed to exec SQL statement: %w", err)
	}

	for _, op := range b.ops {
		switch op.action {
		case batchActionSet:
			_, err := b.tx.Exec(upsertStmt, op.key, op.value, b.version, op.value)
			if err != nil {
				return fmt.Errorf("failed to exec SQL statement: %w", err)
			}

		case batchActionDel:
			_, err := b.tx.Exec(delStmt, b.version, op.key, b.version)
			if err != nil {
				return fmt.Errorf("failed to exec SQL statement: %w", err)
			}
		}
	}

	if err := b.tx.Commit(); err != nil {
		return fmt.Errorf("failed to write SQL transaction: %w", err)
	}

	return nil
}

type iterator struct {
	statement *sql.Stmt
	rows      *sql.Rows
	key, val  []byte
	start     []byte
	valid     bool
	err       error
}

func (db *Database) newIterator(targetVersion uint64, start []byte, reverse bool) (*iterator, error) {
	var (
		keyClause = []string{"version <= ?"}
		queryArgs []any
	)

	keyClause = append(keyClause, "key >= ?")
	queryArgs = []any{targetVersion, start, targetVersion}

	orderBy := "ASC"
	if reverse {
		orderBy = "DESC"
	}

	stmt, err := db.storage.Prepare(fmt.Sprintf(`
	SELECT x.key, x.value
	FROM (
		SELECT key, value, version, tombstone,
			row_number() OVER (PARTITION BY key ORDER BY version DESC) AS _rn
			FROM state_storage WHERE %s
		) x
	WHERE x._rn = 1 AND (x.tombstone = 0 OR x.tombstone > ?) ORDER BY x.key %s;
	`, strings.Join(keyClause, " AND "), orderBy))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare SQL statement: %w", err)
	}

	rows, err := stmt.Query(queryArgs...)
	if err != nil {
		_ = stmt.Close()
		return nil, fmt.Errorf("failed to execute SQL query: %w", err)
	}

	itr := &iterator{
		statement: stmt,
		rows:      rows,
		start:     start,
		valid:     rows.Next(),
	}
	if !itr.valid {
		itr.err = fmt.Errorf("iterator invalid: %w", sql.ErrNoRows)
		return itr, nil
	}

	itr.parseRow()
	if !itr.valid {
		return itr, nil
	}

	return itr, nil
}

func (itr *iterator) Close() {
	_ = itr.statement.Close()
	itr.valid = false
	itr.statement = nil
	itr.rows = nil
}

func (itr *iterator) Key() []byte {
	itr.assertIsValid()
	return slices.Clone(itr.key)
}

func (itr *iterator) Value() []byte {
	itr.assertIsValid()
	return slices.Clone(itr.val)
}

func (itr *iterator) Valid() bool {
	if !itr.valid || itr.rows.Err() != nil {
		itr.valid = false
		return itr.valid
	}

	return true
}

func (itr *iterator) Next() bool {
	if itr.rows.Next() {
		itr.parseRow()
		return itr.Valid()
	}

	itr.valid = false
	return itr.valid
}

func (itr *iterator) parseRow() {
	var (
		key   []byte
		value []byte
	)
	if err := itr.rows.Scan(&key, &value); err != nil {
		itr.err = fmt.Errorf("failed to scan row: %s", err)
		itr.valid = false
		return
	}

	itr.key = key
	itr.val = value
}

func (itr *iterator) assertIsValid() {
	if !itr.valid {
		panic("iterator is invalid")
	}
}
