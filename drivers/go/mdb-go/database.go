package mdb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Database wraps the official mongo-go-driver database with MDB-specific extensions.
type Database struct {
	db *mongo.Database
}

// Collection returns a Collection handle for the given collection name.
func (d *Database) Collection(name string) *Collection {
	return &Collection{coll: d.db.Collection(name)}
}

// ListCollections returns the names of all collections in the database.
func (d *Database) ListCollections(ctx context.Context) ([]string, error) {
	cursor, err := d.db.ListCollections(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("mdb: list collections: %w", err)
	}
	defer cursor.Close(ctx)

	var names []string
	for cursor.Next(ctx) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			return nil, fmt.Errorf("mdb: decode collection name: %w", err)
		}
		if name, ok := result["name"].(string); ok {
			names = append(names, name)
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mdb: cursor error: %w", err)
	}
	return names, nil
}

// CreateCollection creates a new collection with the given name and options.
func (d *Database) CreateCollection(ctx context.Context, name string, opts ...*CreateCollectionOptions) error {
	createOpts := options.CreateCollection()
	if len(opts) > 0 && opts[0] != nil {
		o := opts[0]
		createOpts.SetCapped(o.Capped)
		if o.Capped {
			createOpts.SetSizeInBytes(o.SizeInBytes)
			createOpts.SetMaxDocuments(o.MaxDocuments)
		}
	}

	err := d.db.CreateCollection(ctx, name, createOpts)
	if err != nil {
		return fmt.Errorf("mdb: create collection %q: %w", name, err)
	}
	return nil
}

// DropCollection drops the collection with the given name.
func (d *Database) DropCollection(ctx context.Context, name string) error {
	err := d.db.Collection(name).Drop(ctx)
	if err != nil {
		return fmt.Errorf("mdb: drop collection %q: %w", name, err)
	}
	return nil
}

// RunCommand executes a database command and returns the result.
func (d *Database) RunCommand(ctx context.Context, cmd bson.M) (bson.M, error) {
	var result bson.M
	err := d.db.RunCommand(ctx, cmd).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("mdb: run command: %w", err)
	}
	return result, nil
}

// Stats returns statistics about the database.
func (d *Database) Stats(ctx context.Context) (bson.M, error) {
	var result bson.M
	err := d.db.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("mdb: database stats: %w", err)
	}
	return result, nil
}

// Role represents a database user role.
type Role struct {
	Role string
	DB   string
}

// CreateUser creates a new database user with the given credentials and roles.
func (d *Database) CreateUser(ctx context.Context, username, password string, roles []Role) error {
	bsonRoles := make([]bson.M, len(roles))
	for i, r := range roles {
		bsonRoles[i] = bson.M{"role": r.Role, "db": r.DB}
	}

	cmd := bson.D{
		{Key: "createUser", Value: username},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: bsonRoles},
	}

	err := d.db.RunCommand(ctx, cmd).Err()
	if err != nil {
		return fmt.Errorf("mdb: create user %q: %w", username, err)
	}
	return nil
}

// DropUser removes a database user.
func (d *Database) DropUser(ctx context.Context, username string) error {
	cmd := bson.D{
		{Key: "dropUser", Value: username},
	}

	err := d.db.RunCommand(ctx, cmd).Err()
	if err != nil {
		return fmt.Errorf("mdb: drop user %q: %w", username, err)
	}
	return nil
}
