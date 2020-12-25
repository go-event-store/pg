package pg

import (
	"context"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4"
	pgxpool "github.com/jackc/pgx/v4/pgxpool"
)

type Client struct {
	db *pgxpool.Pool
}

func (c *Client) Conn() interface{} {
	return c.db
}

func (c *Client) Exists(ctx context.Context, collection string) (bool, error) {
	var tablename string

	row := c.db.QueryRow(ctx, `SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1;`, collection)
	err := row.Scan(&tablename)

	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *Client) Delete(ctx context.Context, collection string) error {
	_, err := c.db.Exec(ctx, `DROP TABLE IF EXISTS "`+collection+`";`)

	return err
}

func (c *Client) Reset(ctx context.Context, collection string) error {
	_, err := c.db.Exec(ctx, `TRUNCATE TABLE "`+collection+`" RESTART IDENTITY;`)

	return err
}

func (c *Client) Insert(ctx context.Context, collection string, values map[string]interface{}) error {
	columns := make([]string, 0, len(values))
	placeholder := make([]string, 0, len(values))
	parameters := make([]interface{}, 0, len(values))

	counter := 0

	for column, parameter := range values {
		counter++

		columns = append(columns, column)
		parameters = append(parameters, parameter)
		placeholder = append(placeholder, "$"+strconv.Itoa(counter))
	}

	_, err := c.db.Exec(
		ctx,
		`INSERT INTO "`+collection+`" ("`+strings.Join(columns, `","`)+`") VALUES (`+strings.Join(placeholder, ",")+`);`,
		parameters...,
	)

	return err
}

func (c *Client) Remove(ctx context.Context, collection string, identifiers map[string]interface{}) error {
	conditions := make([]string, 0, len(identifiers))
	parameters := make([]interface{}, 0, len(identifiers))

	counter := 0

	for column, parameter := range identifiers {
		counter++

		parameters = append(parameters, parameter)
		conditions = append(conditions, `"`+column+`" = $`+strconv.Itoa(counter))
	}

	_, err := c.db.Exec(
		ctx,
		`DELETE FROM "`+collection+`" WHERE `+strings.Join(conditions, " AND ")+`;`,
		parameters...,
	)

	return err
}

func (c *Client) Update(ctx context.Context, collection string, values map[string]interface{}, identifiers map[string]interface{}) error {
	conditions := make([]string, 0, len(identifiers))
	updates := make([]string, 0, len(values))
	parameters := make([]interface{}, 0, len(identifiers)+len(values))

	counter := 0

	for column, parameter := range values {
		counter++

		parameters = append(parameters, parameter)
		updates = append(updates, `"`+column+`" = $`+strconv.Itoa(counter))
	}

	for column, parameter := range identifiers {
		counter++

		parameters = append(parameters, parameter)
		conditions = append(conditions, `"`+column+`" = $`+strconv.Itoa(counter))
	}

	_, err := c.db.Exec(
		ctx,
		`UPDATE "`+collection+`" SET `+strings.Join(updates, ",")+` WHERE `+strings.Join(conditions, " AND ")+`;`,
		parameters...,
	)

	return err
}

func NewClient(db *pgxpool.Pool) *Client {
	return &Client{db: db}
}
