# pathsqlx

The path engine implementations in Go for PathQL (see:
[PathQL.org](https://pathql.org/)).

### Important Notes

- **Only tables can have a path** - column paths are not supported
- **Aliases are preserved in the resulting JSON** - any alias specified for
  tables or columns will be used in the output
- **Paths can specify arrays** - if the path ends with [], it's an array;
  otherwise, it's an object (single result)

### Implementation Strategy

pathsqlx uses the **Vitess SQL parser** (via `github.com/xwb1989/sqlparser`) to
analyze SQL queries for automatic path inference and table relationship
detection. The parser extracts:

- Table names and aliases from FROM clauses
- JOIN relationships and ON conditions
- Foreign key relationships for automatic path inference
- PATH hints from SQL comments

### Requirements

- Go 1.11 or higher (uses go modules)
