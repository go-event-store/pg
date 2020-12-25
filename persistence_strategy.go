package pg

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"

	eventstore "github.com/go-event-store/eventstore"
)

const (
	EventStreamsTable = "event_streams"
	ProjectionsTable  = "projections"
)

type PersistenceStrategy struct {
	db *pgxpool.Pool
}

func GenerateTableName(streamName string) string {
	h := sha1.New()
	h.Write([]byte(streamName))

	return "_" + hex.EncodeToString(h.Sum(nil))
}

func (ps PersistenceStrategy) CreateEventStreamsTable(ctx context.Context) error {
	c, err := ps.db.Exec(ctx, `SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1`, EventStreamsTable)
	if err != nil {
		return err
	}

	if c.RowsAffected() == 1 {
		return nil
	}
	_, err = ps.db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			no BIGSERIAL,
			real_stream_name VARCHAR(150) NOT NULL,
			stream_name CHAR(41) NOT NULL,
			metadata JSONB,
			PRIMARY KEY (no),
			UNIQUE (stream_name)
		);`, EventStreamsTable))

	return err
}

func (ps PersistenceStrategy) CreateProjectionsTable(ctx context.Context) error {
	c, err := ps.db.Exec(ctx, `SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1`, ProjectionsTable)
	if err != nil {
		return err
	}
	if c.RowsAffected() == 1 {
		return nil
	}
	_, err = ps.db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
            no BIGSERIAL,
            name VARCHAR(150) NOT NULL,
            position JSONB,
            state JSONB,
            status VARCHAR(28) NOT NULL,
            locked_until TIMESTAMP(6),
            PRIMARY KEY (no),
            UNIQUE (name)
		);`, ProjectionsTable))

	return err
}

func (ps PersistenceStrategy) AddStreamToStreamsTable(ctx context.Context, streamName string) error {
	tableName := GenerateTableName(streamName)
	_, err := ps.db.Exec(ctx, fmt.Sprintf(`INSERT INTO "%s" (real_stream_name, stream_name, metadata) VALUES ($1, $2, $3)`, EventStreamsTable), streamName, tableName, "{}")
	if err != nil {
		switch e := err.(type) {
		case *pgconn.PgError:
			if e.Code == "23505" || e.Code == "23000" {
				return eventstore.StreamAlreadyExist{Stream: streamName}
			}
		default:
			return err
		}
	}
	return nil
}

func (ps PersistenceStrategy) RemoveStreamFromStreamsTable(ctx context.Context, streamName string) error {
	c, err := ps.db.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE real_stream_name = $1`, EventStreamsTable), streamName)
	if c.RowsAffected() == 0 {
		return eventstore.StreamNotFound{Stream: streamName}
	}

	return err
}

func (ps PersistenceStrategy) FetchAllStreamNames(ctx context.Context) ([]string, error) {
	streams := []string{}

	rows, err := ps.db.Query(ctx, fmt.Sprintf(`SELECT real_stream_name FROM %s WHERE real_stream_name NOT LIKE '$%%'`, EventStreamsTable))
	if err != nil {
		return streams, err
	}

	for rows.Next() {
		var stream string

		err = rows.Scan(&stream)
		if err != nil {
			return []string{}, err
		}

		streams = append(streams, stream)
	}

	return streams, nil
}

func (ps PersistenceStrategy) HasStream(ctx context.Context, streamName string) (bool, error) {
	c, err := ps.db.Exec(ctx, fmt.Sprintf(`SELECT real_stream_name FROM %s WHERE real_stream_name = $1`, EventStreamsTable), streamName)
	if err != nil {
		return false, err
	}
	if c.RowsAffected() == 0 {
		return false, nil
	}

	return true, nil
}

func (ps PersistenceStrategy) DeleteStream(ctx context.Context, streamName string) error {
	err := ps.RemoveStreamFromStreamsTable(ctx, streamName)
	if err != nil {
		return err
	}

	return ps.DropSchema(ctx, streamName)
}

func (ps PersistenceStrategy) CreateSchema(ctx context.Context, streamName string) error {
	tableName := GenerateTableName(streamName)
	_, err := ps.db.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			no BIGSERIAL,
			event_id UUID NOT NULL,
			event_name VARCHAR(100) NOT NULL,
			payload JSON NOT NULL,
			metadata JSONB NOT NULL,
			created_at TIMESTAMP(6) NOT NULL,
			PRIMARY KEY (no),
			CONSTRAINT aggregate_version_not_null CHECK ((metadata->>'_aggregate_version') IS NOT NULL),
			CONSTRAINT aggregate_type_not_null CHECK ((metadata->>'_aggregate_type') IS NOT NULL),
			CONSTRAINT aggregate_id_not_null CHECK ((metadata->>'_aggregate_id') IS NOT NULL),
			UNIQUE (event_id)
		);`, tableName))
	if err != nil {
		return err
	}

	_, err = ps.db.Exec(ctx, fmt.Sprintf(`CREATE UNIQUE INDEX ON %s ((metadata->>'_aggregate_type'), (metadata->>'_aggregate_id'), (metadata->>'_aggregate_version'));`, tableName))
	if err != nil {
		return err
	}

	_, err = ps.db.Exec(ctx, fmt.Sprintf(`CREATE UNIQUE INDEX ON %s ((metadata->>'_aggregate_type'), (metadata->>'_aggregate_id'), no);`, tableName))
	if err != nil {
		return err
	}

	return nil
}

func (ps PersistenceStrategy) DropSchema(ctx context.Context, streamName string) error {
	tableName := GenerateTableName(streamName)
	_, err := ps.db.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	if err != nil {
		return err
	}

	return nil
}

func (ps PersistenceStrategy) AppendTo(ctx context.Context, streamName string, events []eventstore.DomainEvent) error {
	tableName := GenerateTableName(streamName)

	tx, err := ps.db.Begin(ctx)
	if err != nil {
		return err
	}

	batch := &pgx.Batch{}

	for _, ev := range events {
		batch.Queue(
			fmt.Sprintf(`INSERT INTO %s (event_id, event_name, payload, metadata, created_at) VALUES ($1, $2, $3, $4, $5)`, tableName),
			ev.UUID().String(),
			ev.Name(),
			ev.Payload(),
			ev.Metadata(),
			ev.CreatedAt(),
		)
	}

	br := tx.SendBatch(ctx, batch)
	err = br.Close()
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}

	return nil
}

func (ps PersistenceStrategy) Load(ctx context.Context, streamName string, fromNumber, count int, matcher eventstore.MetadataMatcher) (eventstore.DomainEventIterator, error) {
	query, values, err := ps.createQuery(ctx, streamName, fromNumber, 0, matcher)
	if err != nil {
		return nil, err
	}

	return NewDomainEventIterator(ctx, ps.db, query, values, count), nil
}

func (ps PersistenceStrategy) MergeAndLoad(ctx context.Context, count int, streams ...eventstore.LoadStreamParameter) (eventstore.DomainEventIterator, error) {
	var paramCounter int
	var queries []string
	var parameters []interface{}

	for _, stream := range streams {
		query, values, err := ps.createQuery(ctx, stream.StreamName, stream.FromNumber, paramCounter, stream.Matcher)
		if err != nil {
			return nil, err
		}

		paramCounter += len(values)
		queries = append(queries, "("+query+")")
		parameters = append(parameters, values...)
	}

	groupedQuery := queries[0]

	if len(queries) > 1 {
		groupedQuery = strings.Join(queries, " UNION ALL ") + " ORDER BY created_at ASC"
	}

	return NewDomainEventIterator(ctx, ps.db, groupedQuery, parameters, count), nil
}

func (ps PersistenceStrategy) createQuery(ctx context.Context, streamName string, fromNumber, paramCounter int, matcher eventstore.MetadataMatcher) (string, []interface{}, error) {
	c, err := ps.db.Exec(ctx, fmt.Sprintf(`SELECT stream_name FROM %s WHERE real_stream_name = $1`, EventStreamsTable), streamName)
	if err != nil {
		return "", []interface{}{}, err
	}
	if c.RowsAffected() == 0 {
		return "", []interface{}{}, eventstore.StreamNotFound{Stream: streamName}
	}

	tableName := GenerateTableName(streamName)

	wheres, values, err := ps.createWhereClause(paramCounter, matcher)

	wheres = append(wheres, fmt.Sprintf(`no >= $%d`, paramCounter+len(values)+1))
	values = append(values, fromNumber)

	whereCondition := fmt.Sprintf(`WHERE %s`, strings.Join(wheres, " AND "))

	query := fmt.Sprintf(`SELECT no, event_id, event_name, payload, metadata, created_at, '%s' as stream FROM %s %s ORDER BY no ASC`, streamName, tableName, whereCondition)

	return query, values, nil
}

func (ps PersistenceStrategy) createWhereClause(paramCounter int, matcher eventstore.MetadataMatcher) ([]string, []interface{}, error) {
	var wheres []string
	var values []interface{}

	if len(matcher) == 0 {
		return wheres, values, nil
	}

	for _, match := range matcher {
		expression := func(value string) string {
			return ""
		}

		switch match.Operation {
		case eventstore.InOperator:
			expression = func(value string) string {
				return fmt.Sprintf("ANY(%s::text[])", value)
			}
		case eventstore.NotInOperator:
			expression = func(value string) string {
				return fmt.Sprintf("NOT IN ANY(%s::text[])", value)
			}
		case eventstore.RegexOperator:
			expression = func(value string) string {
				return fmt.Sprintf("~%s", value)
			}
		default:
			expression = func(value string) string {
				return fmt.Sprintf("%s %s", match.Operation, value)
			}
		}

		if match.FieldType == eventstore.MetadataField {
			switch v := match.Value.(type) {
			case bool:
				wheres = append(wheres, fmt.Sprintf(`metadata->'%s' %s`, match.Field, expression(strconv.FormatBool(v))))
			case int:
				paramCounter++
				values = append(values, match.Value)

				wheres = append(wheres, fmt.Sprintf(`CAST(metadata->>'%s' AS INT) %s`, match.Field, expression(("$"+strconv.Itoa(paramCounter)))))
			default:
				paramCounter++
				values = append(values, match.Value)

				wheres = append(wheres, fmt.Sprintf(`metadata->>'%s' %s`, match.Field, expression(("$"+strconv.Itoa(paramCounter)))))
			}
		}

		if match.FieldType == eventstore.MessagePropertyField {
			switch v := match.Value.(type) {
			case bool:
				wheres = append(wheres, fmt.Sprintf(`%s %s`, match.Field, expression(strconv.FormatBool(v))))
			default:
				paramCounter++
				values = append(values, match.Value)

				wheres = append(wheres, `%s %s`, match.Field, expression(("$" + strconv.Itoa(paramCounter))))
			}
		}
	}

	return wheres, values, nil
}

func NewPersistenceStrategy(db *pgxpool.Pool) *PersistenceStrategy {
	return &PersistenceStrategy{
		db: db,
	}
}
