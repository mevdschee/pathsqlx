package metadata

import (
	"database/sql"
	"fmt"
	"sync"
)

// NewLiveReader creates a MetadataReader that queries the database at runtime.
// Supported drivers: "mysql", "postgres", "sqlite3".
func NewLiveReader(db *sql.DB, driverName string) MetadataReader {
	return &liveReader{
		db:      db,
		driver:  driverName,
		cache:   make(map[string]*TableMetadata),
	}
}

// liveReader implements MetadataReader by querying a live database.
type liveReader struct {
	db      *sql.DB
	driver  string
	cache   map[string]*TableMetadata
	fkCache []ForeignKey
	mu      sync.RWMutex
}

func (r *liveReader) InvalidateCache() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache = make(map[string]*TableMetadata)
	r.fkCache = nil
}

func (r *liveReader) GetTableMetadata(tableName string) (*TableMetadata, error) {
	r.mu.RLock()
	if cached, ok := r.cache[tableName]; ok {
		r.mu.RUnlock()
		return cached, nil
	}
	r.mu.RUnlock()

	a := &analyzer{db: r.db, driver: r.driver}

	columns, err := a.getColumns(tableName)
	if err != nil {
		return nil, err
	}

	pks, err := a.getPrimaryKeys(tableName)
	if err != nil {
		return nil, err
	}

	fks, err := r.GetForeignKeys(tableName)
	if err != nil {
		return nil, err
	}

	meta := &TableMetadata{
		Name:        tableName,
		Columns:     columns,
		PrimaryKeys: pks,
		ForeignKeys: fks,
	}

	r.mu.Lock()
	r.cache[tableName] = meta
	r.mu.Unlock()

	return meta, nil
}

func (r *liveReader) GetForeignKeys(tableName string) ([]ForeignKey, error) {
	allFKs, err := r.GetAllForeignKeys()
	if err != nil {
		return nil, err
	}

	var result []ForeignKey
	for _, fk := range allFKs {
		if fk.FromTable == tableName {
			result = append(result, fk)
		}
	}
	return result, nil
}

func (r *liveReader) GetAllForeignKeys() ([]ForeignKey, error) {
	r.mu.RLock()
	if r.fkCache != nil {
		r.mu.RUnlock()
		return r.fkCache, nil
	}
	r.mu.RUnlock()

	a := &analyzer{db: r.db, driver: r.driver}
	fkDefs, err := a.getAllForeignKeys()
	if err != nil {
		return nil, fmt.Errorf("foreign keys: %w", err)
	}

	fks := make([]ForeignKey, len(fkDefs))
	for i, def := range fkDefs {
		fromTable, fromCol := ParseForeignKeyDef(def.From)
		toTable, toCol := ParseForeignKeyDef(def.To)
		fks[i] = ForeignKey{
			FromTable:  fromTable,
			FromColumn: fromCol,
			ToTable:    toTable,
			ToColumn:   toCol,
		}
	}

	r.mu.Lock()
	r.fkCache = fks
	r.mu.Unlock()

	return fks, nil
}
