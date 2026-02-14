package pathsqlx

import (
	"fmt"
	"regexp"
	"strings"
)

// JoinInfo represents information about a JOIN clause
type JoinInfo struct {
	LeftAlias  string
	LeftTable  string
	RightAlias string
	RightTable string
	JoinType   string // "LEFT", "INNER", "RIGHT", etc.
	Condition  string
	OnColumns  []JoinColumn
}

// JoinColumn represents column information in a join condition
type JoinColumn struct {
	LeftAlias   string
	LeftColumn  string
	RightAlias  string
	RightColumn string
}

// PathHint represents a path hint from SQL comments
type PathHint struct {
	Alias string
	Path  string
}

// QueryAnalysis contains the parsed query structure
type QueryAnalysis struct {
	Tables    map[string]string // alias -> table name
	Joins     []JoinInfo
	PathHints map[string]string // alias -> path override
}

// AnalyzeQuery parses a SQL query to extract structure information
func AnalyzeQuery(sql string) (*QueryAnalysis, error) {
	analysis := &QueryAnalysis{
		Tables:    make(map[string]string),
		Joins:     []JoinInfo{},
		PathHints: make(map[string]string),
	}

	// Extract path hints from comments
	analysis.PathHints = extractPathHints(sql)

	// Extract tables and aliases from FROM clause
	extractFromClause(sql, analysis)

	// Extract JOINs
	extractJoins(sql, analysis)

	return analysis, nil
}

// extractPathHints extracts PATH hints from SQL comments
// Format: -- PATH table_alias $.path or -- PATH: table_alias $.path
// PATH hints apply to table aliases only, not individual columns
// Special case: Use $ as alias for queries without a real table
func extractPathHints(sql string) map[string]string {
	hints := make(map[string]string)

	// Match: -- PATH[:]? table_alias $.path
	// Allow $ alone or followed by word chars, brackets, dots, or asterisks
	// table_alias can be a word or $ for queries without tables
	re := regexp.MustCompile(`--\s*PATH:?\s+(\$|\w+)\s+(\$[\w\[\]\.\*]*)`)
	matches := re.FindAllStringSubmatch(sql, -1)

	for _, match := range matches {
		if len(match) == 3 {
			alias := match[1]
			path := match[2]
			hints[alias] = path
		}
	}

	return hints
}

// extractFromClause extracts table and alias from FROM clause
func extractFromClause(sql string, analysis *QueryAnalysis) {
	// Remove comments
	sql = removeComments(sql)

	// Find FROM clause - stop at WHERE, JOIN, ORDER BY, GROUP BY, LIMIT, or HAVING
	// Pattern: FROM table_name [AS] alias [, table_name [AS] alias]*
	re := regexp.MustCompile(`(?i)FROM\s+(.+?)(?:\s+(?:WHERE|LEFT|RIGHT|INNER|OUTER|JOIN|ORDER|GROUP|LIMIT|HAVING)|$)`)
	matches := re.FindStringSubmatch(sql)

	if len(matches) >= 2 {
		tableList := matches[1]
		// Split by comma to handle multiple tables
		tables := strings.Split(tableList, ",")

		for _, tableSpec := range tables {
			tableSpec = strings.TrimSpace(tableSpec)
			if tableSpec == "" {
				continue
			}

			// Parse: table_name [AS] alias
			tableParts := regexp.MustCompile(`\s+`).Split(tableSpec, -1)
			if len(tableParts) >= 1 {
				tableName := tableParts[0]
				alias := tableName

				// Check for explicit alias
				if len(tableParts) >= 2 {
					// Check if second part is AS keyword
					if len(tableParts) >= 3 && strings.ToUpper(tableParts[1]) == "AS" {
						alias = tableParts[2]
					} else if strings.ToUpper(tableParts[1]) != "AS" {
						// No AS keyword, just alias
						alias = tableParts[1]
					}
				}

				// Make sure the alias is not a SQL keyword
				upperAlias := strings.ToUpper(alias)
				if upperAlias != "WHERE" && upperAlias != "LEFT" && upperAlias != "RIGHT" &&
					upperAlias != "INNER" && upperAlias != "OUTER" && upperAlias != "JOIN" &&
					upperAlias != "ORDER" && upperAlias != "GROUP" && upperAlias != "LIMIT" &&
					upperAlias != "HAVING" {
					analysis.Tables[alias] = tableName
				}
			}
		}
	}
}

// extractJoins extracts JOIN information from the query
func extractJoins(sql string, analysis *QueryAnalysis) {
	// Remove comments
	sql = removeComments(sql)

	// Pattern for JOIN clauses - simplified without lookahead
	// Matches: [LEFT|RIGHT|INNER|OUTER] JOIN table [AS] alias ON condition
	re := regexp.MustCompile(`(?i)(LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+)?JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.+)`)

	matches := re.FindAllStringSubmatch(sql, -1)

	for _, match := range matches {
		joinType := strings.TrimSpace(strings.ToUpper(match[1]))
		if joinType == "" {
			joinType = "INNER"
		} else {
			joinType = strings.TrimSpace(joinType)
		}

		tableName := match[2]
		alias := tableName
		if match[3] != "" {
			alias = match[3]
		}
		condition := strings.TrimSpace(match[4])

		// Trim condition at next JOIN, WHERE, GROUP BY, ORDER BY, or LIMIT
		stopWords := []string{"WHERE", "GROUP BY", "GROUP", "ORDER BY", "ORDER", "LIMIT", "HAVING"}
		for _, word := range stopWords {
			idx := strings.Index(strings.ToUpper(condition), word)
			if idx >= 0 {
				condition = strings.TrimSpace(condition[:idx])
				break
			}
		}

		// Add table to tables map
		analysis.Tables[alias] = tableName

		// Parse join condition
		onColumns := parseJoinCondition(condition)

		// Determine left table from join condition or previous tables
		leftAlias := ""
		leftTable := ""

		if len(onColumns) > 0 {
			// Use the join condition to determine which is left and which is right
			// The right table should be the one we're joining (alias)
			if onColumns[0].RightAlias == alias {
				leftAlias = onColumns[0].LeftAlias
			} else if onColumns[0].LeftAlias == alias {
				leftAlias = onColumns[0].RightAlias
			} else {
				// Neither matches, try to find from previous tables
				for a, t := range analysis.Tables {
					if a != alias {
						leftAlias = a
						leftTable = t
						break
					}
				}
			}

			if leftAlias != "" {
				if lt, ok := analysis.Tables[leftAlias]; ok {
					leftTable = lt
				}
			}
		} else {
			// No parseable condition, use first non-current table
			for a, t := range analysis.Tables {
				if a != alias {
					leftAlias = a
					leftTable = t
					break
				}
			}
		}

		joinInfo := JoinInfo{
			LeftAlias:  leftAlias,
			LeftTable:  leftTable,
			RightAlias: alias,
			RightTable: tableName,
			JoinType:   joinType,
			Condition:  condition,
			OnColumns:  onColumns,
		}

		analysis.Joins = append(analysis.Joins, joinInfo)
	}
}

// parseJoinCondition parses the ON clause to extract column relationships
// Example: "p.id = c.post_id" -> {LeftAlias: "p", LeftColumn: "id", RightAlias: "c", RightColumn: "post_id"}
func parseJoinCondition(condition string) []JoinColumn {
	var columns []JoinColumn

	// Pattern: alias.column = alias.column
	re := regexp.MustCompile(`(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)`)
	matches := re.FindAllStringSubmatch(condition, -1)

	for _, match := range matches {
		if len(match) == 5 {
			col := JoinColumn{
				LeftAlias:   match[1],
				LeftColumn:  match[2],
				RightAlias:  match[3],
				RightColumn: match[4],
			}
			columns = append(columns, col)
		}
	}

	return columns
}

// removeComments removes SQL comments from the query
func removeComments(sql string) string {
	// Remove single-line comments
	re := regexp.MustCompile(`--[^\n]*`)
	sql = re.ReplaceAllString(sql, "")

	// Remove multi-line comments
	re = regexp.MustCompile(`/\*.*?\*/`)
	sql = re.ReplaceAllString(sql, "")

	return sql
}

// GetTableForAlias returns the table name for a given alias
func (a *QueryAnalysis) GetTableForAlias(alias string) (string, bool) {
	table, ok := a.Tables[alias]
	return table, ok
}

// GetJoinForTable returns join information for a table alias
func (a *QueryAnalysis) GetJoinForTable(alias string) *JoinInfo {
	for i := range a.Joins {
		if a.Joins[i].RightAlias == alias {
			return &a.Joins[i]
		}
	}
	return nil
}

// splitRespectingParentheses splits a string by commas while respecting parentheses
func splitRespectingParentheses(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, ch := range s {
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

// parseSelectClause parses the SELECT clause to map columns to their source tables
// Returns a slice of column identifiers (table.column or alias) in the order they appear
func parseSelectClause(query string, analysis *QueryAnalysis) []string {
	// Extract SELECT clause
	reSelect := regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM\s+`)
	matches := reSelect.FindStringSubmatch(query)

	if len(matches) < 2 {
		return []string{}
	}

	selectClause := matches[1]
	columnSources := []string{}

	// Split by comma, but respect parentheses (for subqueries and functions)
	columns := splitRespectingParentheses(selectClause)

	for _, col := range columns {
		col = strings.TrimSpace(col)

		// Check if it has an AS clause
		if strings.Contains(strings.ToUpper(col), " AS ") {
			parts := regexp.MustCompile(`(?i)\s+AS\s+`).Split(col, 2)
			if len(parts) == 2 {
				aliasName := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
				// Use the alias name as the column identifier
				columnSources = append(columnSources, aliasName)
				continue
			}
		}

		// Check if it's table.column format
		if strings.Contains(col, ".") && !strings.Contains(col, "(") {
			parts := strings.SplitN(col, ".", 2)
			if len(parts) == 2 {
				tableAlias := strings.TrimSpace(parts[0])
				colName := strings.TrimSpace(parts[1])
				columnSources = append(columnSources, fmt.Sprintf("%s.%s", tableAlias, colName))
				continue
			}
		}

		// Column without table prefix or expression
		// Try to infer from context
		colName := strings.TrimSpace(col)

		// Check if it's an expression (contains parentheses)
		if strings.Contains(colName, "(") {
			// It's an expression, use as-is (will be handled as a special case)
			columnSources = append(columnSources, colName)
		} else {
			// Simple column name, infer table
			var tableAlias string
			if len(analysis.Tables) == 1 {
				for alias := range analysis.Tables {
					tableAlias = alias
					break
				}
			} else {
				// Find root table
				joinedAliases := make(map[string]bool)
				for _, join := range analysis.Joins {
					joinedAliases[join.RightAlias] = true
				}
				for alias := range analysis.Tables {
					if !joinedAliases[alias] {
						tableAlias = alias
						break
					}
				}
			}

			if tableAlias != "" {
				columnSources = append(columnSources, fmt.Sprintf("%s.%s", tableAlias, colName))
			} else {
				columnSources = append(columnSources, colName)
			}
		}
	}

	return columnSources
}
