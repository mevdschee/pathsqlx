package pathsqlx

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"testing"

	"gopkg.in/gcfg.v1"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var db *DB

var cfg = struct {
	Test struct {
		Username string
		Password string
		Database string
		Driver   string
		Address  string
		Port     string
	}
}{}

func init() {
	var err error
	err = gcfg.ReadFileInto(&cfg, "test_config.ini")
	if err != nil {
		log.Fatalf("Failed to parse gcfg data: %s", err)
	}
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.Test.Address, cfg.Test.Port, cfg.Test.Username, cfg.Test.Password, cfg.Test.Database)
	idb, err := sqlx.Connect(cfg.Test.Driver, dsn)
	if err != nil {
		log.Fatalln(err)
	}
	db = &DB{DB: idb}
}

func TestPathQuery(t *testing.T) {
	type args struct {
		query string
		arg   string
	}
	tests := []struct {
		name    string
		db      *DB
		args    args
		want    string
		wantErr bool
	}{
		{
			"single record no path", db,
			args{"select id, content from posts where id=:id", `{"id": 1}`},
			`[{"id":1,"content":"blog started"}]`, false,
		}, {
			"two records no path", db,
			args{"select id from posts where id<=2 order by id", `{}`},
			`[{"id":1},{"id":2}]`, false,
		}, {
			"two records named no path", db,
			args{"select id from posts where id<=:two and id>=:one order by id", `{"one": 1, "two": 2}`},
			`[{"id":1},{"id":2}]`, false,
		}, {
			"two tables with path", db,
			args{`select posts.id as "$[].posts.id", comments.id as "$[].comments.id" from posts left join comments on post_id = posts.id where posts.id=1`, `{}`},
			`[{"posts":{"id":1},"comments":{"id":1}},{"posts":{"id":1},"comments":{"id":2}}]`, false,
		}, {
			"posts with comments properly nested", db,
			args{`select posts.id as "$.posts[].id", comments.id as "$.posts[].comments[].id" from posts left join comments on post_id = posts.id where posts.id<=2 order by posts.id, comments.id`, `{}`},
			`{"posts":[{"id":1,"comments":[{"id":1},{"id":2}]},{"id":2,"comments":[{"id":3},{"id":4},{"id":5},{"id":6}]}]}`, false,
		}, {
			"comments with post properly nested", db,
			args{`select posts.id as "$.comments[].post.id", comments.id as "$.comments[].id" from posts left join comments on post_id = posts.id where posts.id<=2 order by comments.id, posts.id`, `{}`},
			`{"comments":[{"id":1,"post":{"id":1}},{"id":2,"post":{"id":1}},{"id":3,"post":{"id":2}},{"id":4,"post":{"id":2}},{"id":5,"post":{"id":2}},{"id":6,"post":{"id":2}}]}`, false,
		}, {
			"count posts with simple alias", db,
			args{`select count(*) as "posts" from posts`, `{}`},
			`[{"posts":12}]`, false,
		}, {
			"count posts with path", db,
			args{`select count(*) as "$[].posts" from posts`, `{}`},
			`[{"posts":12}]`, false,
		}, {
			"count posts as object with path", db,
			args{`select count(*) as "$.posts" from posts`, `{}`},
			`{"posts":12}`, false,
		}, {
			"count posts grouped no path", db,
			args{`select categories.name, count(posts.id) as "post_count" from posts, categories where posts.category_id = categories.id group by categories.name order by categories.name`, `{}`},
			`[{"name":"announcement","post_count":11},{"name":"article","post_count":1}]`, false,
		}, {
			"count posts with added root set in path", db,
			args{`select count(*) as "$.statistics.posts" from posts`, `{}`},
			`{"statistics":{"posts":12}}`, false,
		}, {
			"count posts and comments as object with path", db,
			args{`select (select count(*) from posts) as "$.stats.posts", (select count(*) from comments) as "comments"`, `{}`},
			`{"stats":{"posts":12,"comments":6}}`, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var args map[string]interface{}
			err := json.Unmarshal([]byte(tt.args.arg), &args)
			if err != nil {
				log.Fatal("Cannot decode to JSON ", err)
			}
			got, err := tt.db.PathQuery(tt.args.query, args)
			if (err != nil) != tt.wantErr {
				t.Errorf("PathQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			json, err := json.Marshal(got)
			if err != nil {
				log.Fatal("Cannot encode to JSON ", err)
			}
			if !reflect.DeepEqual(string(json), tt.want) {
				t.Errorf("PathQuery() = %v, want %v", string(json), tt.want)
			}
		})
	}
}

func TestConnect(t *testing.T) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.Test.Address, cfg.Test.Port, cfg.Test.Username, cfg.Test.Password, cfg.Test.Database)

	testDb, err := Connect(cfg.Test.Driver, dsn)
	if err != nil {
		t.Errorf("Connect() error = %v", err)
		return
	}
	defer testDb.Close()

	// Verify the connection works
	var result int
	err = testDb.Get(&result, "SELECT 1")
	if err != nil {
		t.Errorf("Connect() produced db that can't query: %v", err)
	}
	if result != 1 {
		t.Errorf("Connect() query returned %d, want 1", result)
	}
}
