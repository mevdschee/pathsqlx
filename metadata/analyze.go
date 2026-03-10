package metadata

import (
	"database/sql"
	"fmt"
	"sort"
)

// Analyze connects to a database and extracts the full schema metadata.
// Supported drivers: "mysql", "postgres", "sqlite3".
func Analyze(db *sql.DB, driverName string) (*Schema, error) {
	a := &analyzer{db: db, driver: driverName}

	tables, err := a.listTables()
	if err != nil {
		return nil, fmt.Errorf("listing tables: %w", err)
	}

	schema := &Schema{
		Tables: make(map[string]Table, len(tables)),
		Paths:  make(map[string]string),
	}

	for _, name := range tables {
		columns, err := a.getColumns(name)
		if err != nil {
			return nil, fmt.Errorf("columns for %s: %w", name, err)
		}

		pks, err := a.getPrimaryKeys(name)
		if err != nil {
			return nil, fmt.Errorf("primary keys for %s: %w", name, err)
		}

		schema.Tables[name] = Table{
			PrimaryKey: pks,
			Columns:    columns,
		}
	}

	fks, err := a.getAllForeignKeys()
	if err != nil {
		return nil, fmt.Errorf("foreign keys: %w", err)
	}
	schema.ForeignKeys = fks

	return schema, nil
}

// AnalyzeAndSave is a convenience function that analyzes a database and
// writes the result to the given path (or DefaultFilename if path is empty).
func AnalyzeAndSave(db *sql.DB, driverName, path string) error {
	if path == "" {
		path = DefaultFilename
	}
	schema, err := Analyze(db, driverName)
	if err != nil {
		return err
	}
	return schema.Save(path)
}

type analyzer struct {
	db     *sql.DB
	driver string
}

// listTables returns all user tables sorted alphabetically.
func (a *analyzer) listTables() ([]string, error) {
	var query string
	switch a.driver {
	case "mysql":
		query = `
			SELECT TABLE_NAME
			FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = DATABASE()
			  AND TABLE_TYPE = 'BASE TABLE'
			ORDER BY TABLE_NAME`
	case "postgres":
		query = `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = 'public'
			  AND table_type = 'BASE TABLE'
			ORDER BY table_name`
	case "sqlite3", "sqlite":
		query = `
			SELECT name
			FROM sqlite_master
			WHERE type = 'table'
			  AND name NOT LIKE 'sqlite_%'
			ORDER BY name`
	default:
		return nil, fmt.Errorf("unsupported driver: %s", a.driver)
	}

	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// getColumns returns column names for a table in ordinal order.
func (a *analyzer) getColumns(table string) ([]string, error) {
	var query string
	var args []interface{}
	switch a.driver {
	case "mysql":
		query = `
			SELECT COLUMN_NAME
			FROM information_schema.COLUMNS
			WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE()
			ORDER BY ORDINAL_POSITION`
		args = []interface{}{table}
	case "postgres":
		query = `
			SELECT column_name
			FROM information_schema.columns
			WHERE table_name = $1 AND table_schema = 'public'
			ORDER BY ordinal_position`
		args = []interface{}{table}
	case "sqlite3", "sqlite":
		query = fmt.Sprintf(`PRAGMA table_info("%s")`, table)
	default:
		return nil, fmt.Errorf("unsupported driver: %s", a.driver)
	}

	if a.driver == "sqlite3" || a.driver == "sqlite" {
		return a.getSQLiteColumns(query)
	}

	rows, err := a.db.Query(query, args...)
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

// getSQLiteColumns parses PRAGMA table_info output.
func (a *analyzer) getSQLiteColumns(query string) ([]string, error) {
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

// getPrimaryKeys returns primary key column names for a table.
func (a *analyzer) getPrimaryKeys(table string) ([]string, error) {
	var query string
	var args []interface{}
	switch a.driver {
	case "mysql":
		query = `
			SELECT COLUMN_NAME
			FROM information_schema.KEY_COLUMN_USAGE
			WHERE TABLE_NAME = ?
			  AND CONSTRAINT_NAME = 'PRIMARY'
			  AND TABLE_SCHEMA = DATABASE()
			ORDER BY ORDINAL_POSITION`
		args = []interface{}{table}
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
			ORDER BY kcu.ordinal_position`
		args = []interface{}{table}
	case "sqlite3", "sqlite":
		return a.getSQLitePrimaryKeys(table)
	default:
		return nil, fmt.Errorf("unsupported driver: %s", a.driver)
	}

	rows, err := a.db.Query(query, args...)
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

// getSQLitePrimaryKeys extracts primary keys from PRAGMA table_info.
func (a *analyzer) getSQLitePrimaryKeys(table string) ([]string, error) {
	query := fmt.Sprintf(`PRAGMA table_info("%s")`, table)
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type pkCol struct {
		name  string
		order int
	}
	var pks []pkCol
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk); err != nil {
			return nil, err
		}
		if pk > 0 {
			pks = append(pks, pkCol{name: name, order: pk})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	sort.Slice(pks, func(i, j int) bool { return pks[i].order < pks[j].order })
	result := make([]string, len(pks))
	for i, pk := range pks {
		result[i] = pk.name
	}
	return result, nil
}

// getAllForeignKeys retrieves all foreign keys from the database.
func (a *analyzer) getAllForeignKeys() ([]ForeignKeyDef, error) {
	switch a.driver {
	case "mysql":
		return a.getMySQLForeignKeys()
	case "postgres":
		return a.getPostgresForeignKeys()
	case "sqlite3", "sqlite":
		return a.getSQLiteForeignKeys()
	default:
		return nil, fmt.Errorf("unsupported driver: %s", a.driver)
	}
}

func (a *analyzer) getMySQLForeignKeys() ([]ForeignKeyDef, error) {
	query := `
		SELECT
			TABLE_NAME,
			COLUMN_NAME,
			REFERENCED_TABLE_NAME,
			REFERENCED_COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE REFERENCED_TABLE_NAME IS NOT NULL
		  AND TABLE_SCHEMA = DATABASE()`

	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fks []ForeignKeyDef
	for rows.Next() {
		var fromTable, fromCol, toTable, toCol string
		if err := rows.Scan(&fromTable, &fromCol, &toTable, &toCol); err != nil {
			return nil, err
		}
		fks = append(fks, ForeignKeyDef{
			From: fromTable + "." + fromCol,
			To:   toTable + "." + toCol,
		})
	}
	return fks, rows.Err()
}

func (a *analyzer) getPostgresForeignKeys() ([]ForeignKeyDef, error) {
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
			AND tc.table_schema = 'public'`

	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fks []ForeignKeyDef
	for rows.Next() {
		var fromTable, fromCol, toTable, toCol string
		if err := rows.Scan(&fromTable, &fromCol, &toTable, &toCol); err != nil {
			return nil, err
		}
		fks = append(fks, ForeignKeyDef{
			From: fromTable + "." + fromCol,
			To:   toTable + "." + toCol,
		})
	}
	return fks, rows.Err()
}

func (a *analyzer) getSQLiteForeignKeys() ([]ForeignKeyDef, error) {
	// First get all table names
	tables, err := a.listTables()
	if err != nil {
		return nil, err
	}

	var fks []ForeignKeyDef
	for _, table := range tables {
		query := fmt.Sprintf(`PRAGMA foreign_key_list("%s")`, table)
		rows, err := a.db.Query(query)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var id, seq int
			var toTable, fromCol, toCol, onUpdate, onDelete, match string
			if err := rows.Scan(&id, &seq, &toTable, &fromCol, &toCol, &onUpdate, &onDelete, &match); err != nil {
				rows.Close()
				return nil, err
			}
			fks = append(fks, ForeignKeyDef{
				From: table + "." + fromCol,
				To:   toTable + "." + toCol,
			})
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return fks, nil
}
