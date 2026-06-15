# pathsqlx

A path engine implementation in Go for PathQL (see:
[PathQL.org](https://pathql.org/)).

### Usage

The `PathQuery` method transforms flat SQL query results into nested JSON
structures:

```go
// Simple query without path hints (automatic inference)
result, err := db.PathQuery(
    `SELECT id, content FROM posts WHERE id <= :id`,
    map[string]interface{}{"id": 2},
    nil, // no hints
)
// Result: [{"id":1,"content":"..."},{"id":2,"content":"..."}]

// With path hints to control nesting
result, err := db.PathQuery(
    `SELECT count(*) AS total FROM posts p`,
    map[string]interface{}{},
    map[string]string{"p": "$"}, // single object result
)
// Result: {"total":2}

// Nested structure with explicit hints
result, err := db.PathQuery(
    `SELECT posts.id, comments.id, comments.message 
     FROM posts LEFT JOIN comments ON comments.post_id = posts.id 
     WHERE posts.id <= :id ORDER BY posts.id, comments.id`,
    map[string]interface{}{"id": 2},
    map[string]string{"posts": "$.posts[]"},
)
// Result: {"posts":[{"id":1,"comments":[...]},{"id":2,"comments":[...]}]}
```

The third parameter is a map of path hints where:

- **Key**: table alias from the SQL query
- **Value**: JSON path (e.g., `$`, `$.posts[]`, `$.stats`)
- **Special alias `$`**: for queries without real tables (e.g., subquery
  results)

### Important Notes

- **Only tables can have a path** - column paths are not supported
- **Aliases are preserved in the resulting JSON** - any alias specified for
  tables or columns will be used in the output
- **Path hints are used verbatim** - if the path ends with `[]` it's an array,
  otherwise it's an object (single result), and `$` is the root object. The
  engine never adds an `[]` to a hint you provide, so include it when a table is
  one-to-many. Tables you do not hint are still nested automatically from the
  foreign keys.

### Algorithm

The path determination follows these steps:

1. **Query Analysis**: The SQL query is parsed using the Vitess SQL parser. It
   identifies tables, their aliases, and how they are joined. Path hints are
   provided as an explicit parameter to the `PathQuery` function.
2. **Cardinality Detection**: For each table, the algorithm determines if it
   represents a "one" or "many" relationship:
   - **Explicit Hints**: A hint is used exactly as written - ending in `[]` makes
     an array and `$` is a single object. The engine does not add `[]` to a hint.
   - **Foreign Keys**: If table B has a foreign key to table A, a join from A to
     B is treated as one-to-many (array).
   - **Join Type**: In the absence of foreign key info, `LEFT JOIN` defaults to
     one-to-many.
   - **Query Defaults**: Queries with `JOIN`s or no hints generally default to
     array results at the root.
3. **Path Generation**: Based on cardinality and join structure:
   - Columns are mapped to paths like `$.table.column` (object) or
     `$.table[].column` (array).
   - Nesting is inferred by following the join tree from the root table.

### Result Transformation

Once paths are determined, the flat database rows are transformed into a nested
JSON structure:

1. **Record Collection**: All rows are fetched from the database, and column
   values are associated with their inferred JSON paths.
2. **Grouping**: Records are split into segments based on array markers (`[]`)
   in their paths.
3. **Entity Hashing**: To handle duplicate data caused by SQL joins (e.g., a
   post appearing multiple times because it has multiple comments), `pathsqlx`
   generates MD5 hashes of the data at each nesting level. This unique
   fingerprint identifies specific entities even when they appear across
   multiple flattened rows.
4. **Tree Merging**: Individual segments are merged into a single nested tree
   structure. The hashes ensure that child entities (like comments) are
   correctly attached to their specific parents (like posts) without duplicating
   the parent data.
5. **Finalization**: The temporary hashes are removed, and the tree is converted
   into standard Go maps and slices, ready for JSON serialization.

### Complete Example

Consider the following query that fetches a post and its comments:

```go
result, err := db.PathQuery(
    `SELECT 
        posts.id, posts.title, posts.content,
        comments.id, comments.message
    FROM 
        posts, comments 
    WHERE 
        comments.post_id = posts.id AND posts.id = :id`,
    map[string]interface{}{"id": 1},
    map[string]string{"posts": "$.posts[]"},
)
```

#### 1. Path Determination

The algorithm evaluates the query structure and database metadata:

- **Query Analysis**: Identifies that `posts` and `comments` are related via the
  `WHERE` clause condition.
- **Cardinality Detection**: Uses foreign key metadata to determine that one
  post can have multiple comments (`one-to-many`).
- **Hint Application**: The hint `{"posts": "$.posts[]"}` directs the engine to
  nest the results under a root `posts` array.
- **Inferred Paths**:
  - `posts` => `$.posts[]` (used verbatim from the hint)
  - `comments` => `$.posts[].comments[]` (no hint of its own, nested inside the
    post array automatically based on the detected relationship)

This results in the following column mapping:

| SQL Column         | JSON Path                      |
| :----------------- | :----------------------------- |
| `posts.id`         | `$.posts[].id`                 |
| `posts.title`      | `$.posts[].title`              |
| `posts.content`    | `$.posts[].content`            |
| `comments.id`      | `$.posts[].comments[].id`      |
| `comments.message` | `$.posts[].comments[].message` |

#### 2. Result Transformation

The database returns flattened rows:

| posts.id | posts.title  | posts.content              | comments.id | comments.message |
| :------- | :----------- | :------------------------- | :---------- | :--------------- |
| 1        | Hello world! | Welcome to the first post. | 1           | Hi!              |
| 1        | Hello world! | Welcome to the first post. | 2           | Thank you.       |

The engine processes these rows:

1. **Grouping**: Detects the `posts[]` and `comments[]` markers.
2. **Entity Hashing**: Generates an MD5 fingerprint for the post data. Both rows
   share this hash because the post ID and title are identical.
3. **Merging**: The rows are merged. Because the post hashes match, they are
   combined into a single object, and the two unique comments are added to its
   `comments` array.
4. **Final Result**: The JSON below is the result of the query.

```json
{
    "posts": [
        {
            "id": 1,
            "title": "Hello world!",
            "content": "Welcome to the first post.",
            "comments": [
                {
                    "id": 1,
                    "message": "Hi!"
                },
                {
                    "id": 2,
                    "message": "Thank you."
                }
            ]
        }
    ]
}
```
