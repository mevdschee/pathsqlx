package metadata

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
)

// DefaultFilename is the default metadata file that pathsqlx looks for.
const DefaultFilename = "pathsqlx.yml"

// ForeignKey represents a foreign key relationship between two tables.
type ForeignKey struct {
	FromTable  string
	FromColumn string
	ToTable    string
	ToColumn   string
}

// TableMetadata represents metadata for a database table.
type TableMetadata struct {
	Name        string
	Columns     []string
	PrimaryKeys []string
	ForeignKeys []ForeignKey
}

// MetadataReader is the interface for reading database metadata.
type MetadataReader interface {
	GetTableMetadata(tableName string) (*TableMetadata, error)
	GetForeignKeys(tableName string) ([]ForeignKey, error)
	GetAllForeignKeys() ([]ForeignKey, error)
	InvalidateCache()
}

// Schema is the top-level structure of a pathsqlx metadata file.
type Schema struct {
	Tables      map[string]Table  `yaml:"tables"`
	ForeignKeys []ForeignKeyDef   `yaml:"foreign_keys"`
	Paths       map[string]string `yaml:"paths,omitempty"`
}

// Table describes a single database table.
type Table struct {
	PrimaryKey []string `yaml:"primary_key"`
	Columns    []string `yaml:"columns"`
}

// ForeignKeyDef describes a foreign key relationship using "table.column" notation.
type ForeignKeyDef struct {
	From string `yaml:"from"`
	To   string `yaml:"to"`
}

// Load reads and parses a metadata file from the given path.
func Load(path string) (*Schema, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading metadata file: %w", err)
	}
	return Parse(data)
}

// LoadDefault tries to load the default metadata file (pathsqlx.yml) from
// the current working directory. Returns nil, nil if the file does not exist.
func LoadDefault() (*Schema, error) {
	if _, err := os.Stat(DefaultFilename); os.IsNotExist(err) {
		return nil, nil
	}
	return Load(DefaultFilename)
}

// Save writes the schema to a YAML file at the given path.
func (s *Schema) Save(path string) error {
	data := s.Marshal()
	return ioutil.WriteFile(path, []byte(data), 0644)
}

// Marshal serializes the schema to YAML. We produce hand-formatted YAML
// for maximum readability rather than relying on a YAML library.
func (s *Schema) Marshal() string {
	var b strings.Builder

	b.WriteString("# pathsqlx metadata — describes database schema for JSON tree construction\n")
	b.WriteString("# see https://github.com/mevdschee/pathsqlx for documentation\n\n")

	// Tables
	b.WriteString("tables:\n")
	for name, table := range s.Tables {
		b.WriteString(fmt.Sprintf("  %s:\n", name))
		b.WriteString(fmt.Sprintf("    primary_key: [%s]\n", strings.Join(table.PrimaryKey, ", ")))
		b.WriteString(fmt.Sprintf("    columns: [%s]\n", strings.Join(table.Columns, ", ")))
		b.WriteString("\n")
	}

	// Foreign keys
	if len(s.ForeignKeys) > 0 {
		b.WriteString("foreign_keys:\n")
		for _, fk := range s.ForeignKeys {
			b.WriteString(fmt.Sprintf("  - from: %s\n", fk.From))
			b.WriteString(fmt.Sprintf("    to: %s\n", fk.To))
			b.WriteString("\n")
		}
	}

	// Paths
	if len(s.Paths) > 0 {
		b.WriteString("paths:\n")
		for alias, path := range s.Paths {
			b.WriteString(fmt.Sprintf("  %s: %s\n", alias, path))
		}
		b.WriteString("\n")
	}

	return b.String()
}

// Parse parses raw YAML bytes into a Schema. This is a minimal parser that
// handles the specific structure of pathsqlx metadata files without
// requiring a third-party YAML library.
func Parse(data []byte) (*Schema, error) {
	schema := &Schema{
		Tables: make(map[string]Table),
		Paths:  make(map[string]string),
	}

	lines := strings.Split(string(data), "\n")
	section := ""         // current top-level key: "tables", "foreign_keys", "paths"
	currentTable := ""    // current table name when inside tables section
	currentTableKey := "" // current key under table: "primary_key", "columns"
	_ = currentTableKey
	inFKItem := false
	var currentFK ForeignKeyDef

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		trimmed := strings.TrimSpace(line)

		// Skip empty lines and comments
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		indent := len(line) - len(strings.TrimLeft(line, " "))

		// Top-level keys (indent 0)
		if indent == 0 {
			if inFKItem && currentFK.From != "" {
				schema.ForeignKeys = append(schema.ForeignKeys, currentFK)
				currentFK = ForeignKeyDef{}
				inFKItem = false
			}
			switch {
			case strings.HasPrefix(trimmed, "tables:"):
				section = "tables"
			case strings.HasPrefix(trimmed, "foreign_keys:"):
				section = "foreign_keys"
			case strings.HasPrefix(trimmed, "paths:"):
				section = "paths"
			}
			currentTable = ""
			continue
		}

		switch section {
		case "tables":
			if indent == 2 && strings.HasSuffix(trimmed, ":") {
				// Table name
				currentTable = strings.TrimSuffix(trimmed, ":")
				if _, ok := schema.Tables[currentTable]; !ok {
					schema.Tables[currentTable] = Table{}
				}
				currentTableKey = ""
			} else if indent == 4 && currentTable != "" {
				key, value := parseKeyValue(trimmed)
				currentTableKey = key
				t := schema.Tables[currentTable]
				switch key {
				case "primary_key":
					t.PrimaryKey = parseBracketList(value)
				case "columns":
					t.Columns = parseBracketList(value)
				}
				schema.Tables[currentTable] = t
			}

		case "foreign_keys":
			if strings.HasPrefix(trimmed, "- ") {
				// New FK item — save previous if any
				if inFKItem && currentFK.From != "" {
					schema.ForeignKeys = append(schema.ForeignKeys, currentFK)
				}
				currentFK = ForeignKeyDef{}
				inFKItem = true
				// Parse "- from: value" on the same line
				inner := strings.TrimPrefix(trimmed, "- ")
				key, value := parseKeyValue(inner)
				if key == "from" {
					currentFK.From = value
				} else if key == "to" {
					currentFK.To = value
				}
			} else if inFKItem {
				key, value := parseKeyValue(trimmed)
				if key == "from" {
					currentFK.From = value
				} else if key == "to" {
					currentFK.To = value
				}
			}

		case "paths":
			if indent == 2 {
				key, value := parseKeyValue(trimmed)
				schema.Paths[key] = value
			}
		}
	}

	// Flush last FK item
	if inFKItem && currentFK.From != "" {
		schema.ForeignKeys = append(schema.ForeignKeys, currentFK)
	}

	return schema, nil
}

// parseKeyValue splits "key: value" into key and value.
func parseKeyValue(s string) (string, string) {
	idx := strings.Index(s, ":")
	if idx < 0 {
		return s, ""
	}
	return strings.TrimSpace(s[:idx]), strings.TrimSpace(s[idx+1:])
}

// parseBracketList parses "[a, b, c]" into []string{"a","b","c"}.
func parseBracketList(s string) []string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// ParseForeignKeyDef splits "table.column" into table and column.
func ParseForeignKeyDef(s string) (string, string) {
	parts := strings.SplitN(s, ".", 2)
	if len(parts) != 2 {
		return s, ""
	}
	return parts[0], parts[1]
}

// NewMetadataReader creates a MetadataReader backed by a Schema loaded from
// a file. This lets the path inference engine work without a live database
// connection.
func (s *Schema) NewMetadataReader() MetadataReader {
	return &fileMetadataReader{schema: s}
}

// fileMetadataReader implements MetadataReader using a Schema.
type fileMetadataReader struct {
	schema *Schema
	mu     sync.RWMutex
}

func (r *fileMetadataReader) GetTableMetadata(tableName string) (*TableMetadata, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, ok := r.schema.Tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %q not found in metadata", tableName)
	}

	fks := r.tableForeignKeys(tableName)

	return &TableMetadata{
		Name:        tableName,
		Columns:     t.Columns,
		PrimaryKeys: t.PrimaryKey,
		ForeignKeys: fks,
	}, nil
}

func (r *fileMetadataReader) GetForeignKeys(tableName string) ([]ForeignKey, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tableForeignKeys(tableName), nil
}

func (r *fileMetadataReader) GetAllForeignKeys() ([]ForeignKey, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]ForeignKey, 0, len(r.schema.ForeignKeys))
	for _, fk := range r.schema.ForeignKeys {
		fromTable, fromCol := ParseForeignKeyDef(fk.From)
		toTable, toCol := ParseForeignKeyDef(fk.To)
		result = append(result, ForeignKey{
			FromTable:  fromTable,
			FromColumn: fromCol,
			ToTable:    toTable,
			ToColumn:   toCol,
		})
	}
	return result, nil
}

func (r *fileMetadataReader) InvalidateCache() {
	// no-op for file-backed reader
}

func (r *fileMetadataReader) tableForeignKeys(tableName string) []ForeignKey {
	var result []ForeignKey
	for _, fk := range r.schema.ForeignKeys {
		fromTable, fromCol := ParseForeignKeyDef(fk.From)
		toTable, toCol := ParseForeignKeyDef(fk.To)
		if fromTable == tableName {
			result = append(result, ForeignKey{
				FromTable:  fromTable,
				FromColumn: fromCol,
				ToTable:    toTable,
				ToColumn:   toCol,
			})
		}
	}
	return result
}
