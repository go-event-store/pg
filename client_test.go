package pg_test

import (
	"context"
	"testing"

	"github.com/go-event-store/pg"
	"github.com/jackc/pgx/v4"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
)

func Test_PostgresClient(t *testing.T) {
	ctx := context.Background()
	db, err := pgxpool.Connect(ctx, "postgres://user:password@localhost/event-store?sslmode=disable")
	if err != nil {
		t.Error(err)
	}

	client := pg.NewClient(db)

	t.Run("Table Handling", func(t *testing.T) {
		exists, err := client.Exists(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}

		if exists {
			t.Fatal("unexisting table should not be found")
		}

		_, err = client.Conn().(*pgxpool.Pool).Exec(ctx, "CREATE TABLE test (name VARCHAR(150) NOT NULL);")
		if err != nil {
			t.Fatal(err)
		}

		exists, err = client.Exists(ctx, "test")
		if !exists {
			t.Fatal("existing table should be found")
		}

		err = client.Delete(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}

		exists, err = client.Exists(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("delete table should not be found")
		}
	})

	t.Run("Table Insert Item", func(t *testing.T) {
		_, err := client.Conn().(*pgxpool.Pool).Exec(ctx, `
			CREATE TABLE insert_test (
				no BIGSERIAL,
				name VARCHAR(150) NOT NULL,
				PRIMARY KEY (no)
			);`)
		if err != nil {
			t.Fatal(err)
		}

		err = client.Insert(ctx, "insert_test", map[string]interface{}{"name": "Rudi"})
		if err != nil {
			t.Fatal(err)
		}

		var no int
		var name string

		err = client.Conn().(*pgxpool.Pool).QueryRow(ctx, "SELECT * FROM insert_test").Scan(&no, &name)
		if err != nil {
			t.Fatal(err)
		}

		if no != 1 {
			t.Error("unexpected number value")
		}

		if name != "Rudi" {
			t.Error("unexpected name value")
		}

		err = client.Delete(ctx, "insert_test")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Table Update Item", func(t *testing.T) {
		_, err := client.Conn().(*pgxpool.Pool).Exec(ctx, `
			CREATE TABLE update_test (
				no BIGSERIAL,
				name VARCHAR(150) NOT NULL,
				PRIMARY KEY (no)
			);`)
		if err != nil {
			t.Fatal(err)
		}

		err = client.Insert(ctx, "update_test", map[string]interface{}{"name": "Rudi"})
		if err != nil {
			t.Fatal(err)
		}

		err = client.Update(ctx, "update_test", map[string]interface{}{"name": "Harald"}, map[string]interface{}{"no": 1})
		if err != nil {
			t.Fatal(err)
		}

		var no int
		var name string

		err = client.Conn().(*pgxpool.Pool).QueryRow(ctx, "SELECT * FROM update_test").Scan(&no, &name)
		if err != nil {
			t.Fatal(err)
		}

		if no != 1 {
			t.Error("unexpected number value")
		}

		if name != "Harald" {
			t.Error("unexpected name value after update")
		}

		err = client.Delete(ctx, "update_test")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Table Remove Item", func(t *testing.T) {
		_, err := client.Conn().(*pgxpool.Pool).Exec(ctx, `
			CREATE TABLE remove_test (
				no BIGSERIAL,
				name VARCHAR(150) NOT NULL,
				PRIMARY KEY (no)
			);`)
		if err != nil {
			t.Fatal(err)
		}

		err = client.Insert(ctx, "remove_test", map[string]interface{}{"name": "Rudi"})
		if err != nil {
			t.Fatal(err)
		}

		err = client.Remove(ctx, "remove_test", map[string]interface{}{"no": 1})
		if err != nil {
			t.Fatal(err)
		}

		err = client.Conn().(*pgxpool.Pool).QueryRow(ctx, "SELECT * FROM remove_test").Scan()
		if err != pgx.ErrNoRows {
			t.Error("No Item should be found")
		}

		err = client.Delete(ctx, "remove_test")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Table Reset", func(t *testing.T) {
		_, err := client.Conn().(*pgxpool.Pool).Exec(ctx, `
			CREATE TABLE reset_test (
				no BIGSERIAL,
				name VARCHAR(150) NOT NULL,
				PRIMARY KEY (no)
			);`)
		if err != nil {
			t.Fatal(err)
		}

		err = client.Insert(ctx, "reset_test", map[string]interface{}{"name": "Rudi"})
		if err != nil {
			t.Fatal(err)
		}

		err = client.Reset(ctx, "reset_test")
		if err != nil {
			t.Fatal(err)
		}

		err = client.Conn().(*pgxpool.Pool).QueryRow(ctx, "SELECT * FROM reset_test").Scan()
		if err != pgx.ErrNoRows {
			t.Error("No Item should be found")
		}

		err = client.Delete(ctx, "reset_test")
		if err != nil {
			t.Fatal(err)
		}
	})
}
