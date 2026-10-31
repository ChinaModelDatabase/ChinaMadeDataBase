package mdb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Transaction represents a multi-document transaction.
type Transaction struct {
	session mongo.Session
	ctx     context.Context
	opts    TransactionOptions
}

// StartTransaction starts a new multi-document transaction.
func (c *Client) StartTransaction(opts ...TransactionOptions) (*Transaction, error) {
	session, err := c.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("mdb: start transaction session: %w", err)
	}

	var tOpts TransactionOptions
	if len(opts) > 0 {
		tOpts = opts[0]
	}

	return &Transaction{
		session: session,
		ctx:     mongo.NewSessionContext(context.Background(), session),
		opts:    tOpts,
	}, nil
}

// Commit commits the transaction.
func (t *Transaction) Commit() error {
	err := t.session.CommitTransaction(t.ctx)
	if err != nil {
		return fmt.Errorf("mdb: commit transaction: %w", err)
	}
	t.session.EndSession(t.ctx)
	return nil
}

// Abort aborts the transaction.
func (t *Transaction) Abort() error {
	err := t.session.AbortTransaction(t.ctx)
	if err != nil {
		return fmt.Errorf("mdb: abort transaction: %w", err)
	}
	t.session.EndSession(t.ctx)
	return nil
}

// Database returns a Database handle within the transaction context.
func (t *Transaction) Database(name string) *Database {
	return &Database{db: mongo.ClientFromSession(t.session).Database(name)}
}

// Collection returns a Collection handle within the transaction context.
func (t *Transaction) Collection(name string) *Collection {
	dbName := ""
	if t.opts.ReadConcern != nil {
		dbName = "admin"
	}
	return &Collection{
		coll: mongo.ClientFromSession(t.session).Database(dbName).Collection(name),
	}
}

// WithTransaction is a convenience method that starts a transaction,
// executes the provided function, and commits or aborts automatically.
func (c *Client) WithTransaction(ctx context.Context, fn func(sessCtx mongo.SessionContext) (interface{}, error)) (interface{}, error) {
	session, err := c.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("mdb: start session for with transaction: %w", err)
	}
	defer session.EndSession(ctx)

	result, err := session.WithTransaction(ctx, fn, options.Transaction().SetReadConcern(options.ReadConcernLocal))
	if err != nil {
		return nil, fmt.Errorf("mdb: with transaction: %w", err)
	}
	return result, nil
}
