# pathsqlx

The path engine implementations in Go for PathQL (see:
[PathQL.org](https://pathql.org/)).

### Important Notes

- **Only tables can have a path** - column paths are not supported
- **Aliases are preserved in the resulting JSON** - any alias specified for
  tables or columns will be used in the output
- **Path hints can specify arrays** - if the path ends with `[]`, it's an array;
  otherwise, it's an object (single result), `$` is the root object.

### Algorithm

The path determination follows these steps:

1.  **Query Analysis**: The SQL query is parsed using the Vitess SQL parser. It identifies tables, their aliases, and how they are joined. It also extracts path hints from SQL comments (e.g., `-- PATH alias $.path`).
2.  **Cardinality Detection**: For each table, the algorithm determines if it represents a "one" or "many" relationship:
    *   **Explicit Hints**: If a `-- PATH` hint ends with `[]`, it's an array. If it's just `$`, it's a single object.
    *   **Foreign Keys**: If table B has a foreign key to table A, a join from A to B is treated as one-to-many (array).
    *   **Join Type**: In the absence of foreign key info, `LEFT JOIN` defaults to one-to-many.
    *   **Query Defaults**: Queries with `JOIN`s or no hints generally default to array results at the root.
3.  **Path Generation**: Based on cardinality and join structure:
    *   Columns are mapped to paths like `$.table.column` (object) or `$.table[].column` (array).
    *   Nesting is inferred by following the join tree from the root table.

### Result Transformation

Once paths are determined, the flat database rows are transformed into a nested JSON structure:

1.  **Record Collection**: All rows are fetched from the database, and column values are associated with their inferred JSON paths.
2.  **Grouping**: Records are split into segments based on array markers (`[]`) in their paths.
3.  **Entity Hashing**: To handle duplicate data caused by SQL joins (e.g., a post appearing multiple times because it has multiple comments), `pathsqlx` generates MD5 hashes of the data at each nesting level. This unique fingerprint identifies specific entities even when they appear across multiple flattened rows.
4.  **Tree Merging**: Individual segments are merged into a single nested tree structure. The hashes ensure that child entities (like comments) are correctly attached to their specific parents (like posts) without duplicating the parent data.
5.  **Finalization**: The temporary hashes are removed, and the tree is converted into standard Go maps and slices, ready for JSON serialization.

### Complete Example

Consider the following query that fetches a post and its comments:

```sql
SELECT 
    posts.id, posts.title, posts.content,
    comments.id, comments.message
FROM 
    posts, comments 
WHERE 
    comments.post_id = posts.id AND posts.id = 1 
-- PATH posts $.posts
```

#### 1. Path Determination
The algorithm evaluates the query structure and database metadata:
*   **Query Analysis**: Identifies that `posts` and `comments` are related via the `WHERE` clause condition.
*   **Cardinality Detection**: Uses foreign key metadata to determine that one post can have multiple comments (`one-to-many`).
*   **Hint Application**: The hint `-- PATH posts $.posts` directs the engine to nest the results under a root `posts` key.
*   **Inferred Paths**:
    *   `posts` => `$.posts[]` (Based on the hint, the `[]` is added because the table is an array)
    *   `comments` => `$.posts[].comments[]` (Automatically nested inside the post object based on the detected relationship)

This results in the following column mapping:

| SQL Column | JSON Path |
| :--- | :--- |
| `posts.id` | `$.posts[].id` |
| `posts.title` | `$.posts[].title` |
| `posts.content` | `$.posts[].content` |
| `comments.id` | `$.posts[].comments[].id` |
| `comments.message` | `$.posts[].comments[].message` |

#### 2. Result Transformation
The database returns flattened rows:
| posts.id | posts.title | posts.content | comments.id | comments.message |
| :--- | :--- | :--- | :--- | :--- |
| 1 | Hello world! | Welcome to the first post. | 1 | Hi! |
| 1 | Hello world! | Welcome to the first post. | 2 | Thank you. |

The engine processes these rows:
1.  **Grouping**: Detects the `posts[]` and `comments[]` markers.
2.  **Entity Hashing**: Generates an MD5 fingerprint for the post data. Both rows share this hash because the post ID and title are identical.
3.  **Merging**: The rows are merged. Because the post hashes match, they are combined into a single object, and the two unique comments are added to its `comments` array.
4.  **Final Result**: The JSON below is the result of the query.

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
