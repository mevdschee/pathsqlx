package pathsqlx

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
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
	metadataReader MetadataReader
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

// splitSelectColumns splits a SELECT clause by commas while respecting parentheses
func splitSelectColumns(selectClause string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, ch := range selectClause {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				// Top-level comma, split here
				result = append(result, current.String())
				current.Reset()
			} else {
				// Inside parentheses, keep the comma
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	// Add the last part
	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

func (db *DB) getPaths(columns []string) ([]string, error) {
	paths := []string{}
	path := "$[]"
	for _, column := range columns {
		prop := column
		if len(column) > 0 && column[0:1] == "$" {
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
			// Convert []byte to appropriate type for proper JSON serialization
			if b, ok := value.([]byte); ok {
				value = convertBytes(b)
			}
			// Strip $ prefix from path, keeping [] markers for structure
			// $[].id → [].id
			// $.id → .id
			path := paths[i]
			if strings.HasPrefix(path, "$") {
				path = path[1:] // Remove "$"
			}
			// Strip [] from the final property name (rightmost segment)
			// [].comments[].id → [].comments[].id (keep structure)
			// But ensure the final key name doesn't include []
			lastDot := strings.LastIndex(path, ".")
			if lastDot >= 0 {
				finalKey := path[lastDot+1:]
				// Remove [] suffix from final key if present
				if strings.HasSuffix(finalKey, "[]") {
					finalKey = finalKey[:len(finalKey)-2]
					path = path[:lastDot+1] + finalKey
				}
			}
			record.Set(path, value)
		}
		records = append(records, record)
	}
	return records, nil
}

// convertBytes converts []byte to the appropriate Go type (int64, float64, or string)
func convertBytes(b []byte) interface{} {
	s := string(b)

	// Try parsing as integer first
	var i int64
	if _, err := fmt.Sscanf(s, "%d", &i); err == nil {
		// Verify it's actually an integer (no extra characters)
		if fmt.Sprintf("%d", i) == s {
			return i
		}
	}

	// Try parsing as float
	var f float64
	if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
		// Only use float if it has a decimal point
		if strings.Contains(s, ".") {
			return f
		}
	}

	// Return as string
	return s
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
	// Initialize metadata reader if not already done
	if db.metadataReader == nil {
		db.metadataReader = NewMetadataReader(db.DB.DB, db.DriverName())
	}

	// Analyze query for structure and hints
	analysis, err := AnalyzeQuery(query)
	if err != nil {
		return nil, err
	}

	rows, err := db.NamedQuery(query, arg)
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Map actual column names to their inferred sources
	columnMapping := make([]string, len(columns))
	hasExplicitPaths := false

	// Build a map of column positions from the query
	// by checking SELECT clause for table.column patterns
	selectPattern := regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+(?:FROM|$)`)
	selectMatches := selectPattern.FindStringSubmatch(query)
	var selectColumns []string
	if len(selectMatches) >= 2 {
		selectClause := selectMatches[1]
		// Remove comments
		commentPattern := regexp.MustCompile(`--[^\n]*`)
		selectClause = commentPattern.ReplaceAllString(selectClause, "")
		// Split by comma respecting parentheses
		selectColumns = splitSelectColumns(selectClause)
	}

	for i, col := range columns {
		// Check if this is an explicit path (starts with $)
		if strings.HasPrefix(col, "$") {
			columnMapping[i] = col
			hasExplicitPaths = true
		} else {
			// Try to match with SELECT clause to find table.column format
			matched := false
			if i < len(selectColumns) {
				selectCol := strings.TrimSpace(selectColumns[i])
				// Check if it's table.column format
				if strings.Contains(selectCol, ".") && !strings.Contains(selectCol, "(") {
					parts := strings.SplitN(selectCol, ".", 2)
					if len(parts) == 2 {
						tableAlias := strings.TrimSpace(parts[0])
						// Verify this table exists in our analysis
						if _, ok := analysis.Tables[tableAlias]; ok {
							columnMapping[i] = tableAlias + "." + col
							matched = true
						}
					}
				}
			}

			if !matched {
				// Column doesn't match table.column pattern, just use as-is
				columnMapping[i] = col
			}
		}
	}

	// If we have explicit paths, use the old getPaths logic
	var paths []string
	if hasExplicitPaths {
		paths, err = db.getPaths(columns)
		if err != nil {
			return nil, err
		}
	} else {
		// Infer paths automatically
		engine := NewPathInferenceEngine(db.metadataReader)
		inferredPaths := engine.InferPathsWithFallback(analysis, columnMapping)

		// Convert to path array format
		paths = make([]string, len(columns))
		for i, col := range columns {
			if i < len(columnMapping) {
				if path, ok := inferredPaths[columnMapping[i]]; ok {
					paths[i] = path
				} else {
					paths[i] = "$[]." + col
				}
			} else {
				paths[i] = "$[]." + col
			}
		}
	}

	records, err := db.getAllRecords(rows, paths)
	if err != nil {
		return nil, err
	}

	// Check if result should be an object (all paths start with "$." not "$[]")
	isObjectResult := true
	hasArrayMarkers := false
	for _, path := range paths {
		if strings.Contains(path, "[]") {
			hasArrayMarkers = true
			isObjectResult = false // Any array marker means we need full pipeline
		}
		if !strings.HasPrefix(path, "$.") || strings.HasPrefix(path, "$[].") || strings.HasPrefix(path, "$[]") {
			isObjectResult = false
		}
	}

	// For object results, simplify the process
	if isObjectResult && len(records) > 0 {
		// Single object result - create nested structure from paths
		result := orderedmap.New()
		for _, key := range records[0].Keys() {
			value, _ := records[0].Get(key)
			// Strip leading dot from key
			if strings.HasPrefix(key, ".") {
				key = key[1:]
			}
			// Create nested structure if key contains dots
			if strings.Contains(key, ".") {
				parts := strings.Split(key, ".")
				current := result
				for _, part := range parts[:len(parts)-1] {
					if _, found := current.Get(part); !found {
						current.Set(part, orderedmap.New())
					}
					next, _ := current.Get(part)
					nextMap, ok := next.(*orderedmap.OrderedMap)
					if !ok {
						// Conflict: trying to nest under a non-object value
						// Create a new map at this level
						nextMap = orderedmap.New()
						current.Set(part, nextMap)
					}
					current = nextMap
				}
				current.Set(parts[len(parts)-1], value)
			} else {
				result.Set(key, value)
			}
		}
		return result, nil
	}

	// For simple array results without grouping (no [] markers), return records as array
	if !hasArrayMarkers {
		results := []interface{}{}
		for _, record := range records {
			obj := orderedmap.New()
			for _, key := range record.Keys() {
				value, _ := record.Get(key)
				obj.Set(key, value)
			}
			results = append(results, obj)
		}
		return results, nil
	}

	// Array results: use the full pipeline
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
	if tree == nil {
		return nil, fmt.Errorf("combineIntoTree returned nil tree")
	}
	result, err := db.removeHashes(tree, "$")
	if err != nil {
		return nil, err
	}
	return result, nil
}
