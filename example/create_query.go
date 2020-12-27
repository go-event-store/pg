package example

import (
	"context"
	"fmt"

	eventstore "github.com/go-event-store/eventstore"
	"github.com/go-event-store/pg"
	"github.com/jackc/pgx/v4/pgxpool"
)

func CreateQuery(ctx context.Context, pool *pgxpool.Pool) {
	typeRegistry := eventstore.NewTypeRegistry()
	typeRegistry.RegisterAggregate(FooAggregate{})
	typeRegistry.RegisterEvents(FooEvent{}, BarEvent{})

	ps := pg.NewPersistenceStrategy(pool)
	es := eventstore.NewEventStore(ps)

	query := eventstore.NewQuery(es)
	err := query.
		FromStream(FooStream, []eventstore.MetadataMatch{}).
		Init(func() interface{} {
			return []string{}
		}).
		When(map[string]eventstore.EventHandler{
			FooEventName: func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				return append(state.([]string), event.Payload().(FooEvent).Foo), nil
			},
			BarEventName: func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
				return append(state.([]string), event.Payload().(BarEvent).Bar), nil
			},
		}).
		Run(ctx)

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(query.State())
}
