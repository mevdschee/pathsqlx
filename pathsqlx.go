package pathsqlx

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/iancoleman/orderedmap"
	"github.com/jmoiron/sqlx"
)

// Type aliases for sqlx types to enable drop-in replacement
type (
	Rows      = sqlx.Rows
	Row       = sqlx.Row
	Tx        = sqlx.Tx
	Stmt      = sqlx.Stmt
	NamedStmt = sqlx.NamedStmt
	Result    = sql.Result
)

// DB is a wrapper around sqlx.DB
type DB struct {
	*sqlx.DB
}

// Open opens a database connection. This is analogous to sql.Open, but returns a *pathsqlx.DB instead.
func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sqlx.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{DB: db}, nil
}

// MustOpen is the same as Open, but panics on error.
func MustOpen(driverName, dataSourceName string) *DB {
	db := sqlx.MustOpen(driverName, dataSourceName)
	return &DB{DB: db}
}

// Connect opens a database connection and verifies with a ping.
func Connect(driverName, dataSourceName string) (*DB, error) {
	db, err := sqlx.Connect(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{DB: db}, nil
}

// ConnectContext opens a database connection and verifies with a ping, using the provided context.
func ConnectContext(ctx context.Context, driverName, dataSourceName string) (*DB, error) {
	db, err := sqlx.ConnectContext(ctx, driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return &DB{DB: db}, nil
}

// MustConnect is the same as Connect, but panics on error.
func MustConnect(driverName, dataSourceName string) *DB {
	db := sqlx.MustConnect(driverName, dataSourceName)
	return &DB{DB: db}
}

// NewDb returns a new pathsqlx.DB wrapper for an existing sql.DB.
func NewDb(db *sql.DB, driverName string) *DB {
	return &DB{DB: sqlx.NewDb(db, driverName)}
}

// ByRevLen is for reverse length-based sort.
type ByRevLen []string

func (a ByRevLen) Len() int           { return len(a) }
func (a ByRevLen) Less(i, j int) bool { return len(a[i]) > len(a[j]) }
func (a ByRevLen) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (db *DB) getPaths(columns []string) ([]string, error) {
	paths := []string{}
	path := "$[]"
	for _, column := range columns {
		prop := column
		if column[0:1] == "$" {
			pos := strings.LastIndex(column, ".")
			if pos != -1 {
				path = column[:pos]
				prop = column[pos+1:]
			}
		}
		paths = append(paths, path+"."+prop)
	}
	return paths, nil
}

func (db *DB) getAllRecords(rows *sqlx.Rows, paths []string) ([]*orderedmap.OrderedMap, error) {
	records := []*orderedmap.OrderedMap{}
	for rows.Next() {
		row, err := rows.SliceScan()
		if err != nil {
			return records, err
		}
		record := orderedmap.New()
		for i, value := range row {
			record.Set(paths[i][1:], value)
		}
		records = append(records, record)
	}
	return records, nil
}

func (db *DB) groupBySeparator(records []*orderedmap.OrderedMap, separator string) ([]*orderedmap.OrderedMap, error) {
	results := []*orderedmap.OrderedMap{}
	for _, record := range records {
		result := orderedmap.New()
		for _, name := range record.Keys() {
			value, _ := record.Get(name)
			parts := strings.Split(name, separator)
			newName := parts[len(parts)-1]
			path := strings.Join(parts[:len(parts)-1], separator)
			if len(parts)-1 > 0 {
				path += separator
			}
			if _, found := result.Get(path); !found {
				result.Set(path, orderedmap.New())
			}
			subResult, _ := result.Get(path)
			subResultMap, _ := subResult.(*orderedmap.OrderedMap)
			subResultMap.Set(newName, value)
		}
		results = append(results, result)
	}
	return results, nil
}

func (db *DB) addHashes(records []*orderedmap.OrderedMap) ([]*orderedmap.OrderedMap, error) {
	results := []*orderedmap.OrderedMap{}
	for _, record := range records {
		mapping := map[string]string{}
		for _, key := range record.Keys() {
			part, _ := record.Get(key)
			if len(key)-2 < 0 || key[len(key)-2:] != "[]" {
				continue
			}
			bytes, err := json.Marshal(part)
			if err != nil {
				return nil, err
			}
			md5 := md5.Sum(bytes)
			hash := hex.EncodeToString(md5[:])
			mapping[key] = key[:len(key)-2] + ".!" + hash + "!"
		}
		mappingKeys := []string{}
		for key := range mapping {
			mappingKeys = append(mappingKeys, key)
		}
		sort.Sort(ByRevLen(mappingKeys))
		result := orderedmap.New()
		for _, key := range record.Keys() {
			value, _ := record.Get(key)
			for _, search := range mappingKeys {
				key = strings.Replace(key, search, mapping[search], -1)
			}
			result.Set(key, value)
		}
		results = append(results, result)
	}
	return results, nil
}

func (db *DB) combineIntoTree(records []*orderedmap.OrderedMap, separator string) (*orderedmap.OrderedMap, error) {
	results := orderedmap.New()
	for _, record := range records {
		for _, name := range record.Keys() {
			value, _ := record.Get(name)
			valueMap, _ := value.(*orderedmap.OrderedMap)
			for _, key := range valueMap.Keys() {
				v, _ := valueMap.Get(key)
				path := strings.Split(name+key, separator)
				newName := path[len(path)-1]
				current := results
				for _, p := range path[:len(path)-1] {
					if _, found := current.Get(p); !found {
						current.Set(p, orderedmap.New())
					}
					next, _ := current.Get(p)
					nextMap, _ := next.(*orderedmap.OrderedMap)
					current = nextMap
				}
				current.Set(newName, v)
			}
		}
	}
	next, _ := results.Get("")
	nextMap, _ := next.(*orderedmap.OrderedMap)
	return nextMap, nil
}

func (db *DB) removeHashes(tree *orderedmap.OrderedMap, path string) (interface{}, error) {
	values := orderedmap.New()
	trees := orderedmap.New()
	results := []interface{}{}
	for _, key := range tree.Keys() {
		value, _ := tree.Get(key)
		valueMap, success := value.(*orderedmap.OrderedMap)
		if success {
			if key[:1] == "!" && key[len(key)-1:] == "!" {
				result, err := db.removeHashes(valueMap, path+"[]")
				if err != nil {
					return nil, err
				}
				results = append(results, result)
			} else {
				result, err := db.removeHashes(valueMap, path+"."+key)
				if err != nil {
					return nil, err
				}
				trees.Set(key, result)
			}
		} else {
			values.Set(key, value)
		}
	}
	if len(results) > 0 {
		hidden := append(values.Keys(), trees.Keys()...)
		if len(hidden) > 0 {
			return nil, fmt.Errorf(
				`The path "%s.%s" is hidden by the path "%s[]"`,
				path, hidden[0], path,
			)
		}
		return results, nil
	}
	mapResults := orderedmap.New()
	for _, key := range values.Keys() {
		value, _ := values.Get(key)
		mapResults.Set(key, value)
	}
	for _, key := range trees.Keys() {
		value, _ := trees.Get(key)
		mapResults.Set(key, value)
	}
	return mapResults, nil
}

// PathQuery is the query that returns nested paths
func (db *DB) PathQuery(query string, arg interface{}) (interface{}, error) {
	rows, err := db.NamedQuery(query, arg)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	paths, err := db.getPaths(columns)
	if err != nil {
		return nil, err
	}
	records, err := db.getAllRecords(rows, paths)
	if err != nil {
		return nil, err
	}
	groups, err := db.groupBySeparator(records, "[]")
	if err != nil {
		return nil, err
	}
	hashes, err := db.addHashes(groups)
	if err != nil {
		return nil, err
	}
	tree, err := db.combineIntoTree(hashes, ".")
	if err != nil {
		return nil, err
	}
	result, err := db.removeHashes(tree, "$")
	if err != nil {
		return nil, err
	}
	return result, nil
}
