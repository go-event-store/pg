package example

import (
	"context"
	"fmt"

	eventstore "github.com/go-event-store/eventstore"
	"github.com/go-event-store/pg"
	"github.com/jackc/pgx/v4/pgxpool"
)

func CreateReadModelProjection(ctx context.Context, pool *pgxpool.Pool) {
	typeRegistry := eventstore.NewTypeRegistry()
	typeRegistry.RegisterAggregate(FooAggregate{})
	typeRegistry.RegisterEvents(FooEvent{}, BarEvent{})

	ps := pg.NewPersistenceStrategy(pool)
	es := eventstore.NewEventStore(ps)
	pm := pg.NewProjectionManager(pool)

	client := pg.NewClient(pool)

	rm := NewFooReadModel(client)

	projector := eventstore.NewReadModelProjector("foo_read_model_projection", rm, es, pm)
	err := projector.
		FromStream(FooStream, []eventstore.MetadataMatch{}).
		Init(func() interface{} {
			return struct{}{}
		}).
		When(map[string]func(state interface{}, event eventstore.DomainEvent) interface{}{
			FooEventName: func(state interface{}, event eventstore.DomainEvent) interface{} {
				projector.ReadModel.Stack(
					"insert",
					map[string]interface{}{
						"id":           event.UUID().String(),
						"aggregate_id": event.AggregateID().String(),
						"value":        event.Payload().(FooEvent).Foo,
					},
				)

				projector.ReadModel.Stack(
					"update",
					map[string]interface{}{
						"value": event.Payload().(FooEvent).Foo,
					},
					map[string]interface{}{
						"aggregate_id": event.AggregateID().String(),
					},
				)

				return state
			},
			BarEventName: func(state interface{}, event eventstore.DomainEvent) interface{} {
				projector.ReadModel.Stack(
					"insert",
					map[string]interface{}{
						"id":           event.UUID().String(),
						"aggregate_id": event.AggregateID().String(),
						"value":        event.Payload().(BarEvent).Bar,
					},
				)

				projector.ReadModel.Stack(
					"remove",
					map[string]interface{}{
						"aggregate_id": event.AggregateID().String(),
					},
				)

				return state
			},
		}).
		Run(ctx, false)

	if err != nil {
		fmt.Println(err)
	}
}
