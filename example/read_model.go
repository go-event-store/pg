package example

import (
	"context"
	"fmt"

	"github.com/go-event-store/pg"
	"github.com/jackc/pgx/v4/pgxpool"
)

const FooReadmodelTable = "app_foo"

type FooReadModel struct {
	client *pg.Client
	stack  []struct {
		method string
		args   []map[string]interface{}
	}
}

func (f *FooReadModel) Init(ctx context.Context) error {
	_, err := f.client.Conn().(*pgxpool.Pool).Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id UUID NOT NULL,
			aggregate_id UUID NOT NULL,
			value VARCHAR(20) NOT NULL,
			PRIMARY KEY (id)
		)`, FooReadmodelTable))

	return err
}

func (f *FooReadModel) IsInitialized(ctx context.Context) (bool, error) {
	return f.client.Exists(ctx, FooReadmodelTable)
}

func (f *FooReadModel) Reset(ctx context.Context) error {
	return f.client.Reset(ctx, FooReadmodelTable)
}

func (f *FooReadModel) Delete(ctx context.Context) error {
	return f.client.Delete(ctx, FooReadmodelTable)
}

func (f *FooReadModel) Stack(method string, args ...map[string]interface{}) {
	f.stack = append(f.stack, struct {
		method string
		args   []map[string]interface{}
	}{method: method, args: args})
}

func (f *FooReadModel) Persist(ctx context.Context) error {
	var err error
	for _, command := range f.stack {
		switch command.method {
		case "insert":
			err = f.client.Insert(ctx, FooReadmodelTable, command.args[0])
			if err != nil {
				return err
			}
		case "remove":
			err = f.client.Remove(ctx, FooReadmodelTable, command.args[0])
			if err != nil {
				return err
			}
		case "update":
			err = f.client.Update(ctx, FooReadmodelTable, command.args[0], command.args[1])
			if err != nil {
				return err
			}
		}
	}

	// reset the stack after persist changes
	f.stack = make([]struct {
		method string
		args   []map[string]interface{}
	}, 0)

	return err
}

func NewFooReadModel(client *pg.Client) *FooReadModel {
	return &FooReadModel{client: client}
}
