package mdb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// Cursor wraps the official mongo-go-driver cursor with MDB-specific extensions.
type Cursor struct {
	cur *mongo.Cursor
}

// Next returns true if there is a document available to decode.
func (c *Cursor) Next(ctx context.Context) bool {
	return c.cur.Next(ctx)
}

// Decode decodes the current document into the provided value.
func (c *Cursor) Decode(val interface{}) error {
	return c.cur.Decode(val)
}

// All iterates the cursor to completion and decodes all documents into results.
func (c *Cursor) All(ctx context.Context, results interface{}) error {
	return c.cur.All(ctx, results)
}

// Close closes the cursor and releases associated resources.
func (c *Cursor) Close(ctx context.Context) error {
	return c.cur.Close(ctx)
}

// Err returns the last error encountered by the cursor.
func (c *Cursor) Err() error {
	return c.cur.Err()
}

// Explain returns the query explanation (explain plan) for the cursor's query.
func (c *Cursor) Explain(ctx context.Context) (bson.M, error) {
	var result bson.M
	err := c.cur.Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("mdb: explain: %w", err)
	}
	return result, nil
}
