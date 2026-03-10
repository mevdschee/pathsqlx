package metadata

import (
	"testing"
)

const testYAML = `tables:
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
`

func TestParseSchema(t *testing.T) {
	schema, err := Parse([]byte(testYAML))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(schema.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(schema.Tables))
	}

	posts := schema.Tables["posts"]
	if len(posts.PrimaryKey) != 1 || posts.PrimaryKey[0] != "id" {
		t.Errorf("posts primary key = %v, want [id]", posts.PrimaryKey)
	}
	if len(posts.Columns) != 5 {
		t.Errorf("posts columns count = %d, want 5", len(posts.Columns))
	}

	if len(schema.ForeignKeys) != 2 {
		t.Fatalf("expected 2 foreign keys, got %d", len(schema.ForeignKeys))
	}
	fk := schema.ForeignKeys[0]
	if fk.From != "comments.post_id" || fk.To != "posts.id" {
		t.Errorf("fk[0] = %s -> %s, want comments.post_id -> posts.id", fk.From, fk.To)
	}

	if schema.Paths["posts"] != "$.posts[]" {
		t.Errorf("paths[posts] = %q, want %q", schema.Paths["posts"], "$.posts[]")
	}
}

func TestMetadataReader(t *testing.T) {
	schema, err := Parse([]byte(testYAML))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	reader := schema.NewMetadataReader()

	// GetTableMetadata
	meta, err := reader.GetTableMetadata("comments")
	if err != nil {
		t.Fatalf("GetTableMetadata: %v", err)
	}
	if meta.Name != "comments" {
		t.Errorf("name = %q, want comments", meta.Name)
	}
	if len(meta.Columns) != 4 {
		t.Errorf("columns count = %d, want 4", len(meta.Columns))
	}

	// GetTableMetadata unknown table
	_, err = reader.GetTableMetadata("nonexistent")
	if err == nil {
		t.Error("expected error for unknown table")
	}

	// GetForeignKeys
	fks, err := reader.GetForeignKeys("comments")
	if err != nil {
		t.Fatalf("GetForeignKeys: %v", err)
	}
	if len(fks) != 1 {
		t.Fatalf("expected 1 FK for comments, got %d", len(fks))
	}
	if fks[0].FromTable != "comments" || fks[0].ToTable != "posts" {
		t.Errorf("FK = %s.%s -> %s.%s", fks[0].FromTable, fks[0].FromColumn, fks[0].ToTable, fks[0].ToColumn)
	}

	// GetAllForeignKeys
	allFKs, err := reader.GetAllForeignKeys()
	if err != nil {
		t.Fatalf("GetAllForeignKeys: %v", err)
	}
	if len(allFKs) != 2 {
		t.Errorf("expected 2 total FKs, got %d", len(allFKs))
	}
}

func TestMarshalRoundtrip(t *testing.T) {
	schema, err := Parse([]byte(testYAML))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	data := schema.Marshal()

	schema2, err := Parse([]byte(data))
	if err != nil {
		t.Fatalf("Parse roundtrip: %v", err)
	}

	if len(schema2.Tables) != len(schema.Tables) {
		t.Errorf("tables count %d != %d", len(schema2.Tables), len(schema.Tables))
	}
	if len(schema2.ForeignKeys) != len(schema.ForeignKeys) {
		t.Errorf("FK count %d != %d", len(schema2.ForeignKeys), len(schema.ForeignKeys))
	}
	if len(schema2.Paths) != len(schema.Paths) {
		t.Errorf("paths count %d != %d", len(schema2.Paths), len(schema.Paths))
	}
}
