# Automatic Path Inference Implementation Plan

## Goal

Transform PathSQLX from requiring explicit column alias paths (e.g., `AS "$.posts[].id"`) to automatically inferring JSON structure from database metadata and SQL query analysis, with optional SQL comment hints for edge cases.

## User Review Required

> [!IMPORTANT]
> **Breaking Change**: This fundamentally changes how PathSQLX works. Existing queries using column alias paths (`AS "$.posts[].id"`) will no longer work and must be rewritten to use automatic inference.

> [!WARNING]
> **Database Metadata Dependency**: Automatic inference requires reading foreign key metadata from the database schema. This adds complexity and potential performance overhead.

> [!CAUTION]
> **SQL Parser Required**: Parsing SQL comments and extracting join information requires either a full SQL parser or regex-based heuristics. Full parsing is complex; heuristics may fail on edge cases.

## Proposed Changes

### Core Components

#### 1. Database Metadata Reader

**Purpose**: Extract foreign key relationships and table structure from the database schema.

##### [NEW] [metadata.go](file:///home/maurits/projects/pathsqlx/metadata.go)

```go
type ForeignKey struct {
    FromTable  string
    FromColumn string
    ToTable    string
    ToColumn   string
}

type TableMetadata struct {
    Name         string
    Columns      []string
    PrimaryKeys  []string
    ForeignKeys  []ForeignKey
}

type MetadataReader interface {
    GetTableMetadata(tableName string) (*TableMetadata, error)
    GetForeignKeys(tableName string) ([]ForeignKey, error)
}
```

**Implementation per database:**
- MySQL/MariaDB: Query `information_schema.KEY_COLUMN_USAGE`
- PostgreSQL: Query `information_schema.table_constraints` and `information_schema.key_column_usage`
- Cache results to avoid repeated queries

---

#### 2. SQL Query Analyzer

**Purpose**: Parse SQL queries to extract table aliases, join conditions, and path hints from comments.

##### [NEW] [query_analyzer.go](file:///home/maurits/projects/pathsqlx/query_analyzer.go)

```go
type JoinInfo struct {
    LeftAlias   string
    LeftTable   string
    RightAlias  string
    RightTable  string
    JoinType    string // "LEFT", "INNER", "RIGHT", etc.
    Condition   string
}

type PathHint struct {
    Alias string
    Path  string
}

type QueryAnalysis struct {
    Tables    map[string]string // alias -> table name
    Joins     []JoinInfo
    PathHints map[string]string // alias -> path override
}

func AnalyzeQuery(sql string) (*QueryAnalysis, error)
```

**Parsing strategy:**
- Extract `-- PATH alias $.path` comments using regex
- Parse FROM clause for table aliases
- Parse JOIN clauses for join relationships
- **Decision needed**: Use full SQL parser (e.g., `vitess/sqlparser`) or regex heuristics?

---

#### 3. Path Inference Engine

**Purpose**: Combine metadata and query analysis to infer JSON paths automatically.

##### [NEW] [path_inference.go](file:///home/maurits/projects/pathsqlx/path_inference.go)

```go
type PathInferenceEngine struct {
    metadata MetadataReader
}

func (e *PathInferenceEngine) InferPaths(
    analysis *QueryAnalysis,
    columns []string,
) (map[string]string, error)
```

**Algorithm:**

1. **Build join graph** from `QueryAnalysis.Joins`
2. **Determine cardinality** for each join:
   - Check FK direction (1:N vs N:1)
   - Use join type (LEFT JOIN suggests optional relationship)
3. **Assign array markers** (`[]`) based on cardinality
4. **Apply path hints** from SQL comments to override defaults
5. **Generate final paths** for each column

**Example logic:**
```
posts p LEFT JOIN comments c ON c.post_id = p.id
→ FK: comments.post_id → posts.id
→ Direction: posts (1) → comments (N)
→ Path: $.p[].c[]
```

---

#### 4. Integration with Existing PathQuery

##### [MODIFY] [pathsqlx.go](file:///home/maurits/projects/pathsqlx/pathsqlx.go)

**Changes to [PathQuery](file:///home/maurits/projects/pathsqlx/pathsqlx.go#280-316) method:**

```go
func (db *DB) PathQuery(query string, arg interface{}) (interface{}, error) {
    // NEW: Analyze query for hints and structure
    analysis, err := AnalyzeQuery(query)
    if err != nil {
        return nil, err
    }
    
    // NEW: Initialize metadata reader (cached)
    if db.metadataReader == nil {
        db.metadataReader = NewMetadataReader(db.DB, db.DriverName())
    }
    
    // Execute query
    rows, err := db.NamedQuery(query, arg)
    if err != nil {
        return nil, err
    }
    
    columns, err := rows.Columns()
    if err != nil {
        return nil, err
    }
    
    // NEW: Infer paths automatically
    engine := &PathInferenceEngine{metadata: db.metadataReader}
    inferredPaths, err := engine.InferPaths(analysis, columns)
    if err != nil {
        return nil, err
    }
    
    // Convert to old path format for existing processing logic
    paths := make([]string, len(columns))
    for i, col := range columns {
        if path, ok := inferredPaths[col]; ok {
            paths[i] = path
        } else {
            paths[i] = "$[]." + col // default fallback
        }
    }
    
    // Continue with existing logic...
    records, err := db.getAllRecords(rows, paths)
    // ... rest unchanged
}
```

**Add to [DB](file:///home/maurits/projects/pathsqlx/pathsqlx.go#28-31) struct:**
```go
type DB struct {
    *sqlx.DB
    metadataReader MetadataReader // NEW: cached metadata
}
```

---

### Testing Strategy

#### Unit Tests

##### [NEW] [metadata_test.go](file:///home/maurits/projects/pathsqlx/metadata_test.go)
- Test FK extraction for MySQL and PostgreSQL
- Test metadata caching

##### [NEW] [query_analyzer_test.go](file:///home/maurits/projects/pathsqlx/query_analyzer_test.go)
- Test parsing of `-- PATH` comments
- Test extraction of table aliases and joins
- Test edge cases (subqueries, CTEs, complex joins)

##### [NEW] [path_inference_test.go](file:///home/maurits/projects/pathsqlx/path_inference_test.go)
- Test 1:N, N:1, N:M relationship inference
- Test path hint overrides
- Test fallback behavior when metadata unavailable

#### Integration Tests

##### [MODIFY] [pathsqlx_test.go](file:///home/maurits/projects/pathsqlx/pathsqlx_test.go)

**Add new test suite:**
```go
func TestAutomaticPathInference(t *testing.T) {
    // Test queries WITHOUT column alias paths
    // Verify automatic inference produces correct JSON
}
```

**Migrate existing tests:**
- Keep current tests as regression suite
- Add equivalent tests using automatic inference
- Document migration path

---

## Verification Plan

### Automated Tests

1. **Run existing test suite** to ensure no regressions
2. **Run new automatic inference tests** across MariaDB and PostgreSQL
3. **Benchmark performance** of metadata reading and path inference

### Manual Verification

1. **Test with real-world schemas** (e.g., php-crud-api database)
2. **Verify path hints work** for edge cases
3. **Test error messages** when inference fails



---

## Open Questions

1. **SQL Parser**: Use full parser (vitess/sqlparser) or regex heuristics?
   - Full parser: More reliable, heavier dependency
   - Regex: Lighter, may fail on complex queries

2. **Metadata Caching**: How long to cache FK metadata?
   - Per-connection lifetime?
   - Configurable TTL?
   - Manual invalidation API?

3. **Error Handling**: What happens when inference fails?
   - Return error?
   - Fall back to flat structure?
   - Require explicit hints?

4. **Performance**: Is metadata lookup acceptable overhead?
   - Benchmark on large schemas
   - Consider lazy loading

5. **Column Ambiguity**: How to handle `SELECT id` without table prefix?
   - Require table prefixes?
   - Use heuristics based on join order?
   - Error if ambiguous?
