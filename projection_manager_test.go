package pg_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	eventstore "github.com/go-event-store/eventstore"
	"github.com/go-event-store/pg"
	"github.com/jackc/pgx/v4/pgxpool"
	uuid "github.com/satori/go.uuid"
)

func Test_PostgresProjectionManager(t *testing.T) {
	ctx := context.Background()
	db, err := pgxpool.Connect(ctx, "postgres://user:password@localhost/event-store?sslmode=disable")
	if err != nil {
		t.Error(err)
	}

	eventStore := eventstore.NewEventStore(pg.NewPersistenceStrategy(db))
	pm := pg.NewProjectionManager(db)
	err = eventStore.Install(ctx)
	if err != nil {
		t.Error(err)
	}

	type TestEvent struct {
		Foo string
	}

	tr := eventstore.NewTypeRegistry()
	tr.RegisterEvents(TestEvent{})

	t.Run("Handle Projection", func(t *testing.T) {
		err := pm.CreateProjection(ctx, "test", map[string]interface{}{"state": 0}, eventstore.StatusIdle)
		if err != nil {
			t.Fatal(err)
		}

		exists, err := pm.ProjectionExists(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatal("Projection should exists")
		}

		_, state, err := pm.LoadProjection(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}

		if testState, ok := state.(map[string]interface{}); ok {
			if value := testState["state"]; value.(float64) != 0 {
				t.Error("unexpected value")
			}
		} else {
			t.Error("unexpected state type")
		}

		err = pm.DeleteProjection(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}

		exists, err = pm.ProjectionExists(ctx, "test")
		if err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("Projection should not exists after delete")
		}
	})

	t.Run("Fetch ProjectionState", func(t *testing.T) {
		err := pm.CreateProjection(ctx, "status", map[string]interface{}{"state": 0}, eventstore.StatusIdle)
		if err != nil {
			t.Fatal(err)
		}

		status, err := pm.FetchProjectionStatus(ctx, "status")
		if err != nil {
			t.Fatal(err)
		}

		if status != eventstore.StatusIdle {
			t.Error("Unexpected Status")
		}

		err = pm.DeleteProjection(ctx, "status")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Update ProjectionState", func(t *testing.T) {
		err := pm.CreateProjection(ctx, "statusUpdate", map[string]interface{}{"state": 0}, eventstore.StatusIdle)
		if err != nil {
			t.Fatal(err)
		}

		err = pm.UpdateProjectionStatus(ctx, "statusUpdate", eventstore.StatusStopping)
		if err != nil {
			t.Fatal(err)
		}

		status, err := pm.FetchProjectionStatus(ctx, "statusUpdate")
		if err != nil {
			t.Fatal(err)
		}

		if status != eventstore.StatusStopping {
			t.Error("Unexpected Status after update")
		}

		err = pm.DeleteProjection(ctx, "statusUpdate")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Reset ProjectionState", func(t *testing.T) {
		err := pm.CreateProjection(ctx, "statusReset", map[string]interface{}{"state": 1}, eventstore.StatusIdle)
		if err != nil {
			t.Fatal(err)
		}

		err = pm.UpdateProjectionStatus(ctx, "statusReset", eventstore.StatusStopping)
		if err != nil {
			t.Fatal(err)
		}

		err = pm.ResetProjection(ctx, "statusReset", map[string]interface{}{"state": 0})
		if err != nil {
			t.Fatal(err)
		}

		status, err := pm.FetchProjectionStatus(ctx, "statusReset")
		if err != nil {
			t.Fatal(err)
		}

		if status != eventstore.StatusIdle {
			t.Error("Unexpected Status after reset")
		}

		_, state, err := pm.LoadProjection(ctx, "statusReset")
		if err != nil {
			t.Fatal(err)
		}

		if testState, ok := state.(map[string]interface{}); ok {
			if value := testState["state"]; value.(float64) != 0 {
				t.Error("unexpected value")
			}
		} else {
			t.Error("unexpected state type")
		}

		err = pm.DeleteProjection(ctx, "statusReset")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Persist ProjectionState", func(t *testing.T) {
		err := pm.CreateProjection(ctx, "statusPersist", map[string]interface{}{"state": 0}, eventstore.StatusIdle)
		if err != nil {
			t.Fatal(err)
		}

		err = pm.PersistProjection(ctx, "statusPersist", map[string]interface{}{"state": 1}, map[string]int{"test": 5})
		if err != nil {
			t.Fatal(err)
		}

		positions, state, err := pm.LoadProjection(ctx, "statusPersist")
		if err != nil {
			t.Fatal(err)
		}

		if testState, ok := state.(map[string]interface{}); ok {
			if value := testState["state"]; value.(float64) != 1 {
				t.Error("unexpected value")
			}
		} else {
			t.Error("unexpected state type")
		}

		if value := positions["test"]; value != 5 {
			t.Error("unexpected position")
		}

		err = pm.DeleteProjection(ctx, "statusPersist")
		if err != nil {
			t.Fatal(err)
		}
	})
}

func Test_PostgresProjector(t *testing.T) {
	type FooEvent struct {
		Foo string
	}

	type BarEvent struct {
		Bar string
	}

	typeRegistry := eventstore.NewTypeRegistry()
	typeRegistry.RegisterEvents(FooEvent{}, BarEvent{})

	ctx := context.Background()
	db, err := pgxpool.Connect(ctx, "postgres://user:password@localhost/event-store?sslmode=disable")
	if err != nil {
		t.Error(err)
	}

	es := eventstore.NewEventStore(pg.NewPersistenceStrategy(db))
	es.Install(ctx)
	es.CreateStream(ctx, "foo-aggregate-stream")

	pm := pg.NewProjectionManager(db)
	aggregateID := uuid.NewV4()

	convert := func(state interface{}) []string {
		nextState := []string{}

		switch s := state.(type) {
		case []interface{}:
			for _, v := range s {
				nextState = append(nextState, fmt.Sprint(v))
			}
		case []string:
			nextState = s
		}

		return nextState
	}

	t.Run("Project all Events", func(t *testing.T) {
		es.AppendTo(ctx, "foo-aggregate-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo1"}, map[string]interface{}{}, time.Now()),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo2"}, map[string]interface{}{}, time.Now()).WithVersion(2),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo3"}, map[string]interface{}{}, time.Now()).WithVersion(3),
			eventstore.NewDomainEvent(uuid.NewV4(), BarEvent{"Bar"}, map[string]interface{}{}, time.Now()),
			eventstore.NewDomainEvent(aggregateID, FooEvent{"Foo4"}, map[string]interface{}{}, time.Now()).WithVersion(4),
		})

		projector := eventstore.NewProjector("project_all", es, pm)
		defer projector.Delete(ctx, false)

		projector.
			Init(func() interface{} {
				return []string{}
			}).
			When(map[string]eventstore.EventHandler{
				"FooEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					return append(convert(state), event.Payload().(FooEvent).Foo), nil
				},
				"BarEvent": func(state interface{}, event eventstore.DomainEvent) (interface{}, error) {
					return append(convert(state), event.Payload().(BarEvent).Bar), nil
				},
			})

		projector.
			FromStream("foo-aggregate-stream", nil).
			Run(ctx, false)

		_, result, err := pm.LoadProjection(ctx, "project_all")
		if err != nil {
			t.Fatal(err)
		}

		state := convert(result)
		if len(state) != 5 {
			t.Fatal("Projection should return a list of all Event Payloads")
		}

		if state[0] != "Foo1" || state[1] != "Foo2" || state[2] != "Foo3" || state[3] != "Bar" || state[4] != "Foo4" {
			t.Error("Projection should return in historical order")
		}
	})
}
