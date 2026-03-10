# pathsqlx Metadata Format

The pathsqlx metadata file describes database schema information that the
path inference engine uses to construct nested JSON trees from flat SQL
results. By providing a metadata file you can avoid runtime schema queries
and version-control your schema alongside your code.

## Default filename

pathsqlx looks for **`pathsqlx.yml`** in the working directory by default.
You can also load from any path using `metadata.Load(path)`.

## File structure

A metadata file has three sections: `tables`, `foreign_keys`, and `paths`.
Only `tables` is required.

```yaml
# pathsqlx metadata — describes database schema for JSON tree construction

tables:
  posts:
    primary_key: [id]
    columns: [id, title, body, category_id, created_at]

  comments:
    primary_key: [id]
    columns: [id, post_id, message, created_at]

  categories:
    primary_key: [id]
    columns: [id, name]

foreign_keys:
  - from: comments.post_id
    to: posts.id

  - from: posts.category_id
    to: categories.id

paths:
  posts: $.posts[]
  comments: $.posts[].comments[]
  categories: $.posts[].category
```

## Sections

### tables

Each key is a table name. Every table has two fields:

| Field         | Type       | Description                                       |
|---------------|------------|---------------------------------------------------|
| `primary_key` | `[string]` | Columns forming the primary key (supports composite keys) |
| `columns`     | `[string]` | All column names in ordinal order                 |

```yaml
tables:
  users:
    primary_key: [id]
    columns: [id, email, name, created_at]

  # composite primary key
  order_items:
    primary_key: [order_id, product_id]
    columns: [order_id, product_id, quantity, price]
```

### foreign_keys

A list of foreign key relationships. Each entry uses compact `table.column`
notation:

| Field  | Type     | Description                              |
|--------|----------|------------------------------------------|
| `from` | `string` | The table and column holding the FK      |
| `to`   | `string` | The referenced table and column          |

```yaml
foreign_keys:
  - from: comments.post_id
    to: posts.id
```

Read this as: *"comments.post_id references posts.id"*. The direction
matters — it tells pathsqlx that `comments` is on the "many" side of the
relationship.

### paths (optional)

Default JSON paths for tables. These serve as fallbacks when no `-- PATH`
hint is provided in a SQL query. Each key is a table name or alias, and the
value is a JSONPath expression.

```yaml
paths:
  posts: $.posts[]
  comments: $.posts[].comments[]
  categories: $.posts[].category
```

Path syntax:
- `$` — the JSON root
- `.name` — an object property
- `[]` — an array (one-to-many relationship)

When omitted, paths are inferred automatically from the foreign key graph and
the SQL query structure (JOIN types, aliases).

## Generating a metadata file

Use `metadata.Analyze` to introspect a live database and produce a schema,
then save it:

```go
import (
    "database/sql"
    "github.com/mevdschee/pathsqlx/metadata"
)

db, _ := sql.Open("mysql", dsn)
// Analyze and write to pathsqlx.yml
metadata.AnalyzeAndSave(db, "mysql", "")

// Or control the process:
schema, _ := metadata.Analyze(db, "postgres")
schema.Save("my-schema.yml")
```

Supported database drivers: `mysql`, `postgres`, `sqlite3`.

## Using a metadata file

Load the file and create a `MetadataReader` that can be used with pathsqlx
without a live database connection for schema queries:

```go
schema, err := metadata.Load("pathsqlx.yml")
if err != nil {
    log.Fatal(err)
}
reader := schema.NewMetadataReader()
```

Or use `LoadDefault()` to try `pathsqlx.yml` in the current directory:

```go
schema, err := metadata.LoadDefault()
// schema is nil if the file does not exist
```
