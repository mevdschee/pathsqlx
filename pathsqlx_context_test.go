package pathsqlx

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// pathQueryCases mirrors the cases exercised by TestPathQuery so the
// context-aware entry points are checked against identical expectations.
var pathQueryCases = []struct {
	name    string
	query   string
	arg     map[string]interface{}
	hints   map[string]string
	want    string
	wantErr bool
}{
	{
		name:  "single record no path",
		query: `SELECT id, content FROM posts WHERE id = :id`,
		arg:   map[string]interface{}{"id": 1},
		hints: nil,
		want:  `[{"id":1,"content":"blog started"}]`,
	},
	{
		name:  "two records no path",
		query: `SELECT id FROM posts WHERE id <= 2 ORDER BY id`,
		arg:   map[string]interface{}{},
		hints: nil,
		want:  `[{"id":1},{"id":2}]`,
	},
	{
		name:  "count as object with path",
		query: `SELECT count(*) AS posts FROM posts p`,
		arg:   map[string]interface{}{},
		hints: map[string]string{"p": "$"},
		want:  `{"posts":2}`,
	},
	{
		name:  "posts with comments nested",
		query: `SELECT posts.id, comments.id FROM posts LEFT JOIN comments ON post_id = posts.id WHERE posts.id <= 2 ORDER BY posts.id, comments.id`,
		arg:   map[string]interface{}{},
		hints: map[string]string{"posts": "$.posts"},
		want:  `{"posts":[{"id":1,"comments":[{"id":1},{"id":2}]},{"id":2,"comments":[{"id":3},{"id":4}]}]}`,
	},
}

func TestPathQueryContext(t *testing.T) {
	for _, dbCfg := range getTestDatabases() {
		t.Run(dbCfg.name, func(t *testing.T) {
			db := setupTestDB(t, dbCfg)
			defer func() {
				db.Exec("DROP TABLE IF EXISTS comments")
				db.Exec("DROP TABLE IF EXISTS posts")
				db.Exec("DROP TABLE IF EXISTS categories")
				db.Close()
			}()

			for _, tt := range pathQueryCases {
				t.Run(tt.name, func(t *testing.T) {
					got, err := db.PathQueryContext(context.Background(), tt.query, tt.arg, tt.hints)
					if (err != nil) != tt.wantErr {
						t.Errorf("PathQueryContext() error = %v, wantErr %v", err, tt.wantErr)
						return
					}
					gotJSON, err := json.Marshal(got)
					if err != nil {
						t.Errorf("PathQueryContext() result cannot be marshaled: %v", err)
						return
					}
					if string(gotJSON) != tt.want {
						t.Errorf("PathQueryContext() = %s, want %s", string(gotJSON), tt.want)
					}
				})
			}
		})
	}
}

func TestPathQueryTx(t *testing.T) {
	for _, dbCfg := range getTestDatabases() {
		t.Run(dbCfg.name, func(t *testing.T) {
			db := setupTestDB(t, dbCfg)
			defer func() {
				db.Exec("DROP TABLE IF EXISTS comments")
				db.Exec("DROP TABLE IF EXISTS posts")
				db.Exec("DROP TABLE IF EXISTS categories")
				db.Close()
			}()

			for _, tt := range pathQueryCases {
				t.Run(tt.name, func(t *testing.T) {
					ctx := context.Background()
					tx, err := db.BeginTxx(ctx, &sql.TxOptions{ReadOnly: true})
					if err != nil {
						t.Fatalf("BeginTxx() error = %v", err)
					}
					defer tx.Rollback()

					got, err := db.PathQueryTx(ctx, tx, tt.query, tt.arg, tt.hints)
					if (err != nil) != tt.wantErr {
						t.Errorf("PathQueryTx() error = %v, wantErr %v", err, tt.wantErr)
						return
					}
					if err := tx.Commit(); err != nil {
						t.Errorf("Commit() error = %v", err)
						return
					}
					gotJSON, err := json.Marshal(got)
					if err != nil {
						t.Errorf("PathQueryTx() result cannot be marshaled: %v", err)
						return
					}
					if string(gotJSON) != tt.want {
						t.Errorf("PathQueryTx() = %s, want %s", string(gotJSON), tt.want)
					}
				})
			}
		})
	}
}

// TestPathQueryTxSetLocal verifies that a SET LOCAL issued on the supplied
// transaction is visible to the query run via PathQueryTx (the whole point of
// the Tx variant). PostgreSQL only.
func TestPathQueryTxSetLocal(t *testing.T) {
	for _, dbCfg := range getTestDatabases() {
		if dbCfg.driver != "postgres" {
			continue
		}
		t.Run(dbCfg.name, func(t *testing.T) {
			db := setupTestDB(t, dbCfg)
			defer func() {
				db.Exec("DROP TABLE IF EXISTS comments")
				db.Exec("DROP TABLE IF EXISTS posts")
				db.Exec("DROP TABLE IF EXISTS categories")
				db.Close()
			}()

			ctx := context.Background()
			tx, err := db.BeginTxx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatalf("BeginTxx() error = %v", err)
			}
			defer tx.Rollback()

			// Set a transaction-local custom GUC; it must be readable by the
			// query run on the same tx.
			if _, err := tx.ExecContext(ctx, "SELECT set_config('app.user', $1, true)", "alice"); err != nil {
				t.Fatalf("set_config error = %v", err)
			}

			got, err := db.PathQueryTx(ctx, tx,
				`SELECT current_setting('app.user', true) AS app_user`,
				map[string]interface{}{},
				map[string]string{"$": "$"})
			if err != nil {
				t.Fatalf("PathQueryTx() error = %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("Commit() error = %v", err)
			}
			gotJSON, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("result cannot be marshaled: %v", err)
			}
			want := `{"app_user":"alice"}`
			if string(gotJSON) != want {
				t.Errorf("PathQueryTx() with SET LOCAL = %s, want %s", string(gotJSON), want)
			}
		})
	}
}

// TestPathQueryContextCancelled verifies that a query issued with an already
// cancelled context returns promptly with an error rather than hanging. This
// needs a real connection (it exercises the driver's context handling), so it
// skips when no test database is reachable.
func TestPathQueryContextCancelled(t *testing.T) {
	for _, dbCfg := range getTestDatabases() {
		t.Run(dbCfg.name, func(t *testing.T) {
			db := setupTestDB(t, dbCfg)
			defer func() {
				db.Exec("DROP TABLE IF EXISTS comments")
				db.Exec("DROP TABLE IF EXISTS posts")
				db.Exec("DROP TABLE IF EXISTS categories")
				db.Close()
			}()

			ctx, cancel := context.WithCancel(context.Background())
			cancel() // cancel before issuing the query

			done := make(chan error, 1)
			go func() {
				_, err := db.PathQueryContext(ctx,
					`SELECT id FROM posts ORDER BY id`,
					map[string]interface{}{}, nil)
				done <- err
			}()

			select {
			case err := <-done:
				if err == nil {
					t.Error("PathQueryContext() with cancelled context returned nil error, want context cancellation error")
				}
			case <-time.After(5 * time.Second):
				t.Error("PathQueryContext() with cancelled context did not return promptly")
			}
		})
	}
}
