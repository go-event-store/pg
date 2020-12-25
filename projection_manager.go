package pg

import (
	"context"
	"fmt"

	eventstore "github.com/go-event-store/eventstore"
	"github.com/jackc/pgx/v4"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
)

type ProjectionManager struct {
	db *pgxpool.Pool
}

func (pm ProjectionManager) FetchProjectionStatus(ctx context.Context, projectionName string) (eventstore.Status, error) {
	var status eventstore.Status

	row := pm.db.QueryRow(ctx, fmt.Sprintf(`SELECT status FROM %s WHERE name = $1;`, ProjectionsTable), projectionName)
	err := row.Scan(&status)

	return status, err
}

func (pm ProjectionManager) CreateProjection(ctx context.Context, projectionName string, state interface{}, status eventstore.Status) error {
	_, err := pm.db.Exec(
		ctx,
		fmt.Sprintf(`INSERT INTO %s (name, position, state, status, locked_until) VALUES ($1, $2, $3, $4, NULL)`, ProjectionsTable),
		projectionName,
		map[string]int{},
		state,
		status,
	)

	return err
}

func (pm ProjectionManager) DeleteProjection(ctx context.Context, projectionName string) error {
	c, err := pm.db.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE "name" = $1`, ProjectionsTable), projectionName)
	if c.RowsAffected() == 0 {
		return eventstore.ProjectionNotFound{Name: projectionName}
	}

	return err
}

func (pm ProjectionManager) ResetProjection(ctx context.Context, projectionName string, state interface{}) error {
	c, err := pm.db.Exec(
		ctx,
		fmt.Sprintf(`UPDATE %s SET status = $1, state = $2, position = $3 WHERE "name" = $4`, ProjectionsTable),
		eventstore.StatusIdle,
		state,
		map[string]int{},
		projectionName,
	)

	if c.RowsAffected() == 0 {
		return eventstore.ProjectionNotFound{Name: projectionName}
	}

	return err
}

func (pm ProjectionManager) PersistProjection(ctx context.Context, projectionName string, state interface{}, streamPositions map[string]int) error {
	c, err := pm.db.Exec(
		ctx,
		fmt.Sprintf(`UPDATE %s SET status = $1, state = $2, position = $3 WHERE "name" = $4`, ProjectionsTable),
		eventstore.StatusIdle,
		state,
		streamPositions,
		projectionName,
	)

	if c.RowsAffected() == 0 {
		return eventstore.ProjectionNotFound{Name: projectionName}
	}

	return err
}

func (pm ProjectionManager) UpdateProjectionStatus(ctx context.Context, projectionName string, status eventstore.Status) error {
	c, err := pm.db.Exec(
		ctx,
		fmt.Sprintf(`UPDATE %s SET status = $1 WHERE "name" = $2`, ProjectionsTable),
		status,
		projectionName,
	)

	if c.RowsAffected() == 0 {
		return eventstore.ProjectionNotFound{Name: projectionName}
	}

	return err
}

func (pm ProjectionManager) LoadProjection(ctx context.Context, projectionName string) (map[string]int, interface{}, error) {
	position := map[string]int{}
	var state interface{}

	row := pm.db.QueryRow(ctx, fmt.Sprintf(`SELECT position, state FROM %s WHERE name = $1 LIMIT 1`, ProjectionsTable), projectionName)
	err := row.Scan(&position, &state)
	if err == pgx.ErrNoRows {
		return position, state, eventstore.ProjectionNotFound{Name: projectionName}
	}

	return position, state, err
}

func (pm ProjectionManager) ProjectionExists(ctx context.Context, projectionName string) (bool, error) {
	var name string

	row := pm.db.QueryRow(ctx, fmt.Sprintf(`SELECT name FROM %s WHERE name = $1;`, ProjectionsTable), projectionName)
	err := row.Scan(&name)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, err
}

func NewProjectionManager(db *pgxpool.Pool) *ProjectionManager {
	return &ProjectionManager{db: db}
}
