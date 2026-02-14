package pathsqlx

import (
	"fmt"
	"strings"
)

// PathInferenceEngine infers JSON paths from query structure
type PathInferenceEngine struct {
	metadata MetadataReader
}

// NewPathInferenceEngine creates a new path inference engine
func NewPathInferenceEngine(metadata MetadataReader) *PathInferenceEngine {
	return &PathInferenceEngine{
		metadata: metadata,
	}
}

// InferPaths generates JSON paths for query columns based on metadata and query structure
func (e *PathInferenceEngine) InferPaths(analysis *QueryAnalysis, columns []string) (map[string]string, error) {
	paths := make(map[string]string)

	// Build cardinality map for each table alias
	cardinality, err := e.buildCardinalityMap(analysis)
	if err != nil {
		return nil, err
	}

	// Process each column
	for _, col := range columns {
		path, err := e.inferColumnPath(col, analysis, cardinality)
		if err != nil {
			return nil, err
		}
		paths[col] = path
	}

	return paths, nil
}

// buildCardinalityMap determines whether each table in the query is one-to-many
func (e *PathInferenceEngine) buildCardinalityMap(analysis *QueryAnalysis) (map[string]bool, error) {
	cardinality := make(map[string]bool)

	// Get all foreign keys
	allFKs, err := e.metadata.GetAllForeignKeys()
	if err != nil {
		return nil, err
	}

	// Find the root table (the one in the FROM clause that's not joined)
	rootAlias := ""
	joinedAliases := make(map[string]bool)
	for _, join := range analysis.Joins {
		joinedAliases[join.RightAlias] = true
	}

	// Root is the table that's not the right side of any join
	for alias := range analysis.Tables {
		if !joinedAliases[alias] {
			rootAlias = alias
			break
		}
	}

	// Special case: if there's a PATH hint for $ (root), use it
	if hintPath, ok := analysis.PathHints["$"]; ok {
		if rootAlias == "" {
			// No real table, use $ as the alias
			rootAlias = "$"
			analysis.Tables["$"] = "$"
		}
		// Apply the hint to the root alias
		analysis.PathHints[rootAlias] = hintPath
	}

	// Determine if the result should be an array at the root level
	// Cardinality is determined by query structure:
	// 1. If PATH hint explicitly includes [] → array
	// 2. If PATH hint is exactly "$" → single object
	// 3. If there are JOINs → array (join expansion creates multiple rows)
	// 4. No PATH hint and no JOINs → array (default: multiple rows)
	// 5. PATH hint like "$.something" with JOINs → array
	// 6. PATH hint like "$.something" without JOINs → single object (e.g., aggregates)
	if rootAlias != "" {
		// Check for PATH hint
		if hintPath, ok := analysis.PathHints[rootAlias]; ok {
			// If path ends with [], it's explicitly an array
			if strings.HasSuffix(hintPath, "[]") {
				cardinality[rootAlias] = true
			} else if hintPath == "$" {
				// Exactly "$" means single object at root
				cardinality[rootAlias] = false
			} else {
				// Path like "$.something" - check if there are joins
				cardinality[rootAlias] = len(analysis.Joins) > 0
			}
		} else {
			// No PATH hint: default to array (queries return multiple rows)
			cardinality[rootAlias] = true
		}
	}

	// For each join, determine if it's one-to-many or many-to-one
	for _, join := range analysis.Joins {
		isArray := e.isOneToManyJoin(join, allFKs)
		cardinality[join.RightAlias] = isArray
	}

	// For tables without joins (implicit joins via comma), set them as arrays too
	// unless they have explicit PATH hints
	for alias := range analysis.Tables {
		if _, exists := cardinality[alias]; !exists {
			// No cardinality set yet - default to array
			cardinality[alias] = true
		}
	}

	return cardinality, nil
}

// isOneToManyJoin determines if a join represents a one-to-many relationship
func (e *PathInferenceEngine) isOneToManyJoin(join JoinInfo, allFKs []ForeignKey) bool {
	// If no join columns parsed, assume LEFT JOIN implies array
	if len(join.OnColumns) == 0 {
		return join.JoinType == "LEFT" || join.JoinType == "LEFT OUTER"
	}

	// Check if there's a FK from right table to left table
	// If so, it's a one-to-many (left -> many right)
	for _, jc := range join.OnColumns {
		for _, fk := range allFKs {
			// Check if right table has FK to left table
			if fk.FromTable == join.RightTable && fk.ToTable == join.LeftTable {
				if (jc.RightAlias == join.RightAlias && jc.RightColumn == fk.FromColumn) ||
					(jc.LeftAlias == join.RightAlias && jc.LeftColumn == fk.FromColumn) {
					// Right table has FK to left = one-to-many
					return true
				}
			}

			// Check if left table has FK to right table
			if fk.FromTable == join.LeftTable && fk.ToTable == join.RightTable {
				if (jc.LeftAlias == join.LeftAlias && jc.LeftColumn == fk.FromColumn) ||
					(jc.RightAlias == join.LeftAlias && jc.RightColumn == fk.FromColumn) {
					// Left table has FK to right = many-to-one
					return false
				}
			}
		}
	}

	// Default: if LEFT JOIN, treat as array
	return join.JoinType == "LEFT" || join.JoinType == "LEFT OUTER"
}

// inferColumnPath generates the JSON path for a single column
// PATH hints apply only to table aliases, not individual columns
func (e *PathInferenceEngine) inferColumnPath(column string, analysis *QueryAnalysis, cardinality map[string]bool) (string, error) {
	// Parse column format: "alias.column" or "column"
	parts := strings.Split(column, ".")

	var alias, colName string
	if len(parts) == 2 {
		alias = parts[0]
		colName = parts[1]

		// Check for path hint for the table alias
		if hintPath, ok := analysis.PathHints[alias]; ok {
			// Need to add array marker if this table has multiple rows
			if cardinality[alias] {
				// Check if hint already ends with []
				if !strings.HasSuffix(hintPath, "[]") {
					return hintPath + "[]." + colName, nil
				}
			}
			return hintPath + "." + colName, nil
		}
	} else {
		// No alias - try to infer from available tables
		colName = column
		alias = e.guessAliasForColumn(colName, analysis)

		// If no table could be determined, this might be an expression or subquery result
		if alias == "" {
			// For columns without a table source (expressions, subqueries, etc.)
			// check if there's a special PATH hint for $ (root)
			if hintPath, ok := analysis.PathHints["$"]; ok {
				return hintPath + "." + colName, nil
			}
			// Default: place at root level as object property
			return "$." + colName, nil
		}

		// Check for path hint for the table alias
		if hintPath, ok := analysis.PathHints[alias]; ok {
			// Need to add array marker if this table has multiple rows
			if cardinality[alias] {
				// Check if hint already ends with []
				if !strings.HasSuffix(hintPath, "[]") {
					return hintPath + "[]." + colName, nil
				}
			}
			return hintPath + "." + colName, nil
		}
	}

	// For simple queries without joins, use simple paths
	if len(analysis.Joins) == 0 {
		if cardinality[alias] {
			return "$[]." + colName, nil
		}
		return "$." + colName, nil
	}

	// For queries with joins, need to check if we have path hints
	// If the root table has a path hint, nested tables should be relative to it
	rootAlias := e.findRootAlias(analysis)
	if rootHint, ok := analysis.PathHints[rootAlias]; ok {
		// Build path relative to the root hint
		if alias == rootAlias {
			// This is the root table
			if cardinality[alias] {
				if !strings.HasSuffix(rootHint, "[]") {
					return rootHint + "[]." + colName, nil
				}
			}
			return rootHint + "." + colName, nil
		} else {
			// This is a joined table - nest it under root
			if cardinality[rootAlias] {
				// Root is array
				if cardinality[alias] {
					// Nested table is also array
					if !strings.HasSuffix(rootHint, "[]") {
						return rootHint + "[]." + alias + "[]." + colName, nil
					}
					return rootHint + "." + alias + "[]." + colName, nil
				}
				// Nested table is object
				if !strings.HasSuffix(rootHint, "[]") {
					return rootHint + "[]." + alias + "." + colName, nil
				}
				return rootHint + "." + alias + "." + colName, nil
			} else {
				// Root is object
				if cardinality[alias] {
					return rootHint + "." + alias + "[]." + colName, nil
				}
				return rootHint + "." + alias + "." + colName, nil
			}
		}
	}

	// No path hints - use default behavior
	path := e.buildPathToTable(alias, analysis, cardinality)
	return path + "." + colName, nil
}

// findRootAlias finds the root table alias (the one not on right side of any join)
func (e *PathInferenceEngine) findRootAlias(analysis *QueryAnalysis) string {
	joinedAliases := make(map[string]bool)
	for _, join := range analysis.Joins {
		joinedAliases[join.RightAlias] = true
	}
	for alias := range analysis.Tables {
		if !joinedAliases[alias] {
			return alias
		}
	}
	return ""
}

// guessAliasForColumn tries to determine which table a column belongs to
func (e *PathInferenceEngine) guessAliasForColumn(column string, analysis *QueryAnalysis) string {
	// Return the first table if only one exists
	if len(analysis.Tables) == 1 {
		for alias := range analysis.Tables {
			return alias
		}
	}

	// Try to find the column in table metadata
	for alias, tableName := range analysis.Tables {
		metadata, err := e.metadata.GetTableMetadata(tableName)
		if err != nil {
			continue
		}
		for _, col := range metadata.Columns {
			if col == column {
				return alias
			}
		}
	}

	// Default to first table
	for alias := range analysis.Tables {
		return alias
	}

	return ""
}

// buildPathToTable constructs the JSON path from root to a specific table
func (e *PathInferenceEngine) buildPathToTable(targetAlias string, analysis *QueryAnalysis, cardinality map[string]bool) string {
	// Start with root
	path := "$"

	// Find path through joins
	visited := make(map[string]bool)
	path = e.buildPathRecursive(targetAlias, analysis, cardinality, visited)

	return path
}

// buildPathRecursive recursively builds the path by following joins
func (e *PathInferenceEngine) buildPathRecursive(targetAlias string, analysis *QueryAnalysis, cardinality map[string]bool, visited map[string]bool) string {
	if visited[targetAlias] {
		return ""
	}
	visited[targetAlias] = true

	// Find the root table alias (the one that's not on the right side of any join)
	rootAlias := ""
	joinedAliases := make(map[string]bool)
	for _, join := range analysis.Joins {
		joinedAliases[join.RightAlias] = true
	}
	for alias := range analysis.Tables {
		if !joinedAliases[alias] {
			rootAlias = alias
			break
		}
	}

	// Check if this is the root table
	isRoot := (targetAlias == rootAlias)

	if isRoot {
		// This is the root table
		if cardinality[targetAlias] {
			return "$[]." + targetAlias
		}
		return "$." + targetAlias
	}

	// This is a joined table - place it at the root level (flat structure)
	// For flat joins, all tables are siblings within each result row
	if cardinality[rootAlias] {
		// Root is an array, so tables are within each array element
		if cardinality[targetAlias] {
			// This table is one-to-many relative to parent, so it gets an array marker
			return "$[]." + targetAlias + "[]"
		}
		return "$[]." + targetAlias
	}

	// Root is object
	if cardinality[targetAlias] {
		return "$." + targetAlias + "[]"
	}
	return "$." + targetAlias
}

// InferPathsWithFallback is a helper that provides fallback behavior
func (e *PathInferenceEngine) InferPathsWithFallback(analysis *QueryAnalysis, columns []string) map[string]string {
	paths, err := e.InferPaths(analysis, columns)
	if err != nil {
		// Fallback: create simple flat paths
		paths = make(map[string]string)
		for _, col := range columns {
			paths[col] = "$[]." + col
		}
	}
	return paths
}

// ValidatePaths checks if inferred paths are valid
func (e *PathInferenceEngine) ValidatePaths(paths map[string]string) error {
	// Check for conflicting paths (e.g., both $.x and $.x[] exist)
	pathMap := make(map[string]bool)

	for _, path := range paths {
		// Extract just the path without the final property
		parts := strings.Split(path, ".")
		if len(parts) > 1 {
			basePath := strings.Join(parts[:len(parts)-1], ".")

			// Check if both array and non-array versions exist
			arrayPath := basePath + "[]"
			if pathMap[basePath] && strings.Contains(path, "[]") {
				return fmt.Errorf("conflicting paths: both %s and %s[] exist", basePath, basePath)
			}
			if pathMap[arrayPath] && !strings.Contains(path, "[]") {
				return fmt.Errorf("conflicting paths: both %s and %s[] exist", basePath, basePath)
			}

			pathMap[path] = true
		}
	}

	return nil
}
