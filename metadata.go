package pathsqlx

import (
	"database/sql"
	"fmt"
	"sync"
)

// ForeignKey represents a foreign key relationship
type ForeignKey struct {
	FromTable  string
	FromColumn string
	ToTable    string
	ToColumn   string
}

// TableMetadata represents metadata for a database table
type TableMetadata struct {
	Name        string
	Columns     []string
	PrimaryKeys []string
	ForeignKeys []ForeignKey
}

// MetadataReader interface for reading database metadata
type MetadataReader interface {
	GetTableMetadata(tableName string) (*TableMetadata, error)
	GetForeignKeys(tableName string) ([]ForeignKey, error)
	GetAllForeignKeys() ([]ForeignKey, error)
	InvalidateCache()
}

// metadataReaderImpl is the concrete implementation
type metadataReaderImpl struct {
	db         *sql.DB
	driverName string
	cache      map[string]*TableMetadata
	fkCache    []ForeignKey
	mu         sync.RWMutex
}

// NewMetadataReader creates a new MetadataReader
func NewMetadataReader(db *sql.DB, driverName string) MetadataReader {
	return &metadataReaderImpl{
		db:         db,
		driverName: driverName,
		cache:      make(map[string]*TableMetadata),
	}
}

// InvalidateCache clears the metadata cache
func (r *metadataReaderImpl) InvalidateCache() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache = make(map[string]*TableMetadata)
	r.fkCache = nil
}

// GetTableMetadata retrieves metadata for a specific table
func (r *metadataReaderImpl) GetTableMetadata(tableName string) (*TableMetadata, error) {
	// Check cache first
	r.mu.RLock()
	if cached, ok := r.cache[tableName]; ok {
		r.mu.RUnlock()
		return cached, nil
	}
	r.mu.RUnlock()

	// Fetch from database
	metadata := &TableMetadata{
		Name: tableName,
	}

	// Get columns
	columns, err := r.getColumns(tableName)
	if err != nil {
		return nil, err
	}
	metadata.Columns = columns

	// Get primary keys
	pks, err := r.getPrimaryKeys(tableName)
	if err != nil {
		return nil, err
	}
	metadata.PrimaryKeys = pks

	// Get foreign keys
	fks, err := r.GetForeignKeys(tableName)
	if err != nil {
		return nil, err
	}
	metadata.ForeignKeys = fks

	// Cache the result
	r.mu.Lock()
	r.cache[tableName] = metadata
	r.mu.Unlock()

	return metadata, nil
}

// GetForeignKeys retrieves foreign keys for a specific table
func (r *metadataReaderImpl) GetForeignKeys(tableName string) ([]ForeignKey, error) {
	allFKs, err := r.GetAllForeignKeys()
	if err != nil {
		return nil, err
	}

	result := []ForeignKey{}
	for _, fk := range allFKs {
		if fk.FromTable == tableName {
			result = append(result, fk)
		}
	}
	return result, nil
}

// GetAllForeignKeys retrieves all foreign keys from the database
func (r *metadataReaderImpl) GetAllForeignKeys() ([]ForeignKey, error) {
	// Check cache first
	r.mu.RLock()
	if r.fkCache != nil {
		r.mu.RUnlock()
		return r.fkCache, nil
	}
	r.mu.RUnlock()

	var fks []ForeignKey
	var err error

	switch r.driverName {
	case "mysql":
		fks, err = r.getMySQLForeignKeys()
	case "postgres":
		fks, err = r.getPostgresForeignKeys()
	default:
		return nil, fmt.Errorf("unsupported driver: %s", r.driverName)
	}

	if err != nil {
		return nil, err
	}

	// Cache the result
	r.mu.Lock()
	r.fkCache = fks
	r.mu.Unlock()

	return fks, nil
}

// getMySQLForeignKeys retrieves foreign keys from MySQL/MariaDB
func (r *metadataReaderImpl) getMySQLForeignKeys() ([]ForeignKey, error) {
	query := `
		SELECT 
			TABLE_NAME,
			COLUMN_NAME,
			REFERENCED_TABLE_NAME,
			REFERENCED_COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE REFERENCED_TABLE_NAME IS NOT NULL
		AND TABLE_SCHEMA = DATABASE()
	`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fks []ForeignKey
	for rows.Next() {
		var fk ForeignKey
		err := rows.Scan(&fk.FromTable, &fk.FromColumn, &fk.ToTable, &fk.ToColumn)
		if err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}

	return fks, rows.Err()
}

// getPostgresForeignKeys retrieves foreign keys from PostgreSQL
func (r *metadataReaderImpl) getPostgresForeignKeys() ([]ForeignKey, error) {
	query := `
		SELECT
			tc.table_name,
			kcu.column_name,
			ccu.table_name AS foreign_table_name,
			ccu.column_name AS foreign_column_name
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.key_column_usage AS kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
		JOIN information_schema.constraint_column_usage AS ccu
			ON ccu.constraint_name = tc.constraint_name
			AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_schema = 'public'
	`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fks []ForeignKey
	for rows.Next() {
		var fk ForeignKey
		err := rows.Scan(&fk.FromTable, &fk.FromColumn, &fk.ToTable, &fk.ToColumn)
		if err != nil {
			return nil, err
		}
		fks = append(fks, fk)
	}

	return fks, rows.Err()
}

// getColumns retrieves column names for a table
func (r *metadataReaderImpl) getColumns(tableName string) ([]string, error) {
	var query string
	switch r.driverName {
	case "mysql":
		query = `
			SELECT COLUMN_NAME
			FROM information_schema.COLUMNS
			WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE()
			ORDER BY ORDINAL_POSITION
		`
	case "postgres":
		query = `
			SELECT column_name
			FROM information_schema.columns
			WHERE table_name = $1 AND table_schema = 'public'
			ORDER BY ordinal_position
		`
	default:
		return nil, fmt.Errorf("unsupported driver: %s", r.driverName)
	}

	rows, err := r.db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// getPrimaryKeys retrieves primary key columns for a table
func (r *metadataReaderImpl) getPrimaryKeys(tableName string) ([]string, error) {
	var query string
	switch r.driverName {
	case "mysql":
		query = `
			SELECT COLUMN_NAME
			FROM information_schema.KEY_COLUMN_USAGE
			WHERE TABLE_NAME = ? 
			AND CONSTRAINT_NAME = 'PRIMARY'
			AND TABLE_SCHEMA = DATABASE()
			ORDER BY ORDINAL_POSITION
		`
	case "postgres":
		query = `
			SELECT kcu.column_name
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
				ON tc.constraint_name = kcu.constraint_name
				AND tc.table_schema = kcu.table_schema
			WHERE tc.constraint_type = 'PRIMARY KEY'
				AND tc.table_name = $1
				AND tc.table_schema = 'public'
			ORDER BY kcu.ordinal_position
		`
	default:
		return nil, fmt.Errorf("unsupported driver: %s", r.driverName)
	}

	rows, err := r.db.Query(query, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pks []string
	for rows.Next() {
		var pk string
		if err := rows.Scan(&pk); err != nil {
			return nil, err
		}
		pks = append(pks, pk)
	}

	return pks, rows.Err()
}
