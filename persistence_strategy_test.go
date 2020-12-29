package pg_test

import (
	"context"
	"testing"
	"time"

	eventstore "github.com/go-event-store/eventstore"
	"github.com/go-event-store/pg"
	"github.com/jackc/pgx/v4/pgxpool"
	uuid "github.com/satori/go.uuid"
)

func Test_PostgresEventStore(t *testing.T) {
	ctx := context.Background()
	db, err := pgxpool.Connect(ctx, "postgres://user:password@localhost/event-store?sslmode=disable")
	if err != nil {
		t.Error(err)
	}

	eventStore := eventstore.NewEventStore(pg.NewPersistenceStrategy(db))
	err = eventStore.Install(ctx)
	if err != nil {
		t.Error(err)
	}

	type TestEvent struct {
		Foo string
	}

	tr := eventstore.NewTypeRegistry()
	tr.RegisterEvents(TestEvent{})

	t.Run("Double Install has no effect", func(t *testing.T) {
		err = eventStore.Install(ctx)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Create EventStream", func(t *testing.T) {
		err := eventStore.CreateStream(ctx, "foo-stream")
		if err != nil {
			t.Error(err)
		}

		ok, err := eventStore.HasStream(ctx, "foo-stream")
		if err != nil {
			t.Error(err)
		}

		if ok == false {
			t.Error("Expected EventStream not found")
		}

		err = eventStore.DeleteStream(ctx, "foo-stream")
		if err != nil {
			t.Error(err)
		}

		ok, err = eventStore.HasStream(ctx, "foo-stream")
		if err != nil {
			t.Error(err)
		}

		if ok == true {
			t.Error("Expected EventStream not was deleted")
		}
	})

	t.Run("AppendTo EventStream", func(t *testing.T) {
		err := eventStore.CreateStream(ctx, "foo-stream")
		if err != nil {
			t.Error(err)
		}

		uuid1 := uuid.NewV4()
		uuid2 := uuid.NewV4()

		err = eventStore.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(uuid1, TestEvent{}, map[string]interface{}{}, time.Now()),
			eventstore.NewDomainEvent(uuid2, TestEvent{Foo: "test"}, map[string]interface{}{}, time.Now()),
		})
		if err != nil {
			t.Error(err)
		}

		it, err := eventStore.Load(ctx, "foo-stream", 1, 0, nil)
		if err != nil {
			t.Error(err)
		}

		ev1, err := it.Current()
		if err != nil {
			t.Error(err)
		}
		if ev1.AggregateID() != uuid1 {
			t.Error("Expected first appended Event")
		}

		it.Next()

		ev2, err := it.Current()
		if err != nil {
			t.Error(err)
		}
		if ev2.AggregateID() != uuid2 {
			t.Error("Expected second appended Event")
		}

		err = eventStore.DeleteStream(ctx, "foo-stream")
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Load Empty EventStream", func(t *testing.T) {
		err := eventStore.CreateStream(ctx, "foo-stream")
		if err != nil {
			t.Error(err)
		}

		it, err := eventStore.Load(ctx, "foo-stream", 1, 0, nil)
		if err != nil {
			t.Error(err)
		}

		ok, err := it.IsEmpty()
		if err != nil {
			t.Error(err)
		}
		if ok == false {
			t.Error("Expected empty Iterator")
		}

		err = eventStore.DeleteStream(ctx, "foo-stream")
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Load from not existing Stream returns StreamNotFound", func(t *testing.T) {
		_, err := eventStore.Load(ctx, "bar-stream", 1, 0, nil)
		if _, ok := err.(eventstore.StreamNotFound); ok == false {
			t.Errorf("Expected a StreamNotFound error")
		}
	})
	t.Run("MergeAndLoad from multiple Streams", func(t *testing.T) {
		err := eventStore.CreateStream(ctx, "foo-stream")
		if err != nil {
			t.Error(err)
		}

		err = eventStore.CreateStream(ctx, "bar-stream")
		if err != nil {
			t.Error(err)
		}

		uuid1 := uuid.NewV4()
		uuid2 := uuid.NewV4()
		uuid3 := uuid.NewV4()

		err = eventStore.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(uuid1, TestEvent{}, map[string]interface{}{}, time.Now()),
		})
		if err != nil {
			t.Error(err)
		}
		err = eventStore.AppendTo(ctx, "bar-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(uuid2, TestEvent{}, map[string]interface{}{}, time.Now()),
		})
		if err != nil {
			t.Error(err)
		}
		err = eventStore.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.NewDomainEvent(uuid3, TestEvent{}, map[string]interface{}{}, time.Now()),
		})
		if err != nil {
			t.Error(err)
		}

		it, err := eventStore.MergeAndLoad(ctx, 0, []eventstore.LoadStreamParameter{
			{StreamName: "foo-stream", FromNumber: 1},
			{StreamName: "bar-stream", FromNumber: 1},
		}...)
		if err != nil {
			t.Error(err)
			return
		}

		ok, err := it.IsEmpty()
		if err != nil {
			t.Error(err)
			return
		}

		if ok == true {
			t.Error("Iterator should have result")
			return
		}

		ev1, _ := it.Current()
		if ev1.AggregateID() != uuid1 {
			t.Error("Expected first appended Event")
		}

		it.Next()

		ev2, _ := it.Current()
		if ev2.AggregateID() != uuid2 {
			t.Error("Expected second appended Event")
		}

		it.Next()

		ev3, _ := it.Current()
		if ev3.AggregateID() != uuid3 {
			t.Error("Expected second appended Event")
		}

		eventStore.DeleteStream(ctx, "foo-stream")
		eventStore.DeleteStream(ctx, "bar-stream")
	})

	t.Run("Load from Stream with bool and integer Metamatcher", func(t *testing.T) {
		err := eventStore.CreateStream(ctx, "foo-stream")
		if err != nil {
			t.Error(err)
		}
		defer eventStore.DeleteStream(ctx, "foo-stream")

		uuid1 := uuid.NewV4()
		uuid2 := uuid.NewV4()

		err = eventStore.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.
				NewDomainEvent(uuid1, TestEvent{}, nil, time.Now()).
				WithAddedMetadata("bool", true).
				WithAddedMetadata("integer", 2),
		})
		if err != nil {
			t.Error(err)
		}
		err = eventStore.AppendTo(ctx, "foo-stream", []eventstore.DomainEvent{
			eventstore.
				NewDomainEvent(uuid2, TestEvent{}, nil, time.Now()).
				WithAddedMetadata("bool", true).
				WithAddedMetadata("integer", 3),
		})
		if err != nil {
			t.Error(err)
		}

		it, err := eventStore.Load(ctx, "foo-stream", 0, 0, []eventstore.MetadataMatch{
			{
				Field:     "integer",
				FieldType: eventstore.MetadataField,
				Value:     2,
				Operation: eventstore.LowerThanEuqalsOperator,
			},
			{
				Field:     "bool",
				FieldType: eventstore.MetadataField,
				Value:     true,
				Operation: eventstore.EqualsOperator,
			},
		})
		if err != nil {
			t.Error(err)
			return
		}

		ok, err := it.IsEmpty()
		if err != nil {
			t.Error(err)
			return
		}

		if ok == true {
			t.Error("Iterator should have result")
			return
		}

		ev1, _ := it.Current()
		if ev1.AggregateID() != uuid1 {
			t.Error("Expected first appended Event")
		}

		hasNext := it.Next()
		if hasNext {
			t.Error("Expected only one result Event")
		}
	})
}
