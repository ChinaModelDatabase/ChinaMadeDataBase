package mdb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Collection wraps the official mongo-go-driver collection with MDB-specific extensions.
type Collection struct {
	coll *mongo.Collection
}

// Find performs a find query and returns a cursor over the matching documents.
func (c *Collection) Find(ctx context.Context, filter interface{}, opts ...*FindOptions) (*Cursor, error) {
	mongoOpts := options.Find()
	if len(opts) > 0 && opts[0] != nil {
		o := opts[0]
		if o.Limit != 0 {
			mongoOpts.SetLimit(o.Limit)
		}
		if o.Skip != 0 {
			mongoOpts.SetSkip(o.Skip)
		}
		if o.Sort != nil {
			mongoOpts.SetSort(o.Sort)
		}
		if o.Projection != nil {
			mongoOpts.SetProjection(o.Projection)
		}
	}

	cursor, err := c.coll.Find(ctx, filter, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: find: %w", err)
	}
	return &Cursor{cur: cursor}, nil
}

// FindOne performs a find query and returns a SingleResult.
func (c *Collection) FindOne(ctx context.Context, filter interface{}, opts ...*FindOneOptions) (*SingleResult, error) {
	mongoOpts := options.FindOne()
	if len(opts) > 0 && opts[0] != nil {
		o := opts[0]
		if o.Projection != nil {
			mongoOpts.SetProjection(o.Projection)
		}
		if o.Sort != nil {
			mongoOpts.SetSort(o.Sort)
		}
	}

	result := c.coll.FindOne(ctx, filter, mongoOpts)
	if result.Err() != nil {
		return nil, fmt.Errorf("mdb: find one: %w", result.Err())
	}
	return &SingleResult{result: result}, nil
}

// SingleResult represents the result of a FindOne operation.
type SingleResult struct {
	result *mongo.SingleResult
}

// Decode decodes the SingleResult into the provided value.
func (sr *SingleResult) Decode(v interface{}) error {
	return sr.result.Decode(v)
}

// InsertOne inserts a single document and returns the insert result.
func (c *Collection) InsertOne(ctx context.Context, doc interface{}, opts ...*InsertOneOptions) (*InsertOneResult, error) {
	mongoOpts := options.InsertOne()

	result, err := c.coll.InsertOne(ctx, doc, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: insert one: %w", err)
	}
	return &InsertOneResult{InsertedID: result.InsertedID}, nil
}

// InsertOneResult represents the result of an InsertOne operation.
type InsertOneResult struct {
	InsertedID interface{}
}

// InsertMany inserts multiple documents and returns the insert result.
func (c *Collection) InsertMany(ctx context.Context, docs []interface{}, opts ...*InsertManyOptions) (*InsertManyResult, error) {
	mongoOpts := options.InsertMany()
	if len(opts) > 0 && opts[0] != nil {
		mongoOpts.SetOrdered(opts[0].Ordered)
	}

	result, err := c.coll.InsertMany(ctx, docs, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: insert many: %w", err)
	}
	return &InsertManyResult{InsertedIDs: result.InsertedIDs}, nil
}

// InsertManyResult represents the result of an InsertMany operation.
type InsertManyResult struct {
	InsertedIDs []interface{}
}

// UpdateOne updates a single document matching the filter.
func (c *Collection) UpdateOne(ctx context.Context, filter, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	mongoOpts := options.UpdateOne()

	result, err := c.coll.UpdateOne(ctx, filter, update, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: update one: %w", err)
	}
	return &UpdateResult{
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		UpsertedID:    result.UpsertedID,
	}, nil
}

// UpdateMany updates multiple documents matching the filter.
func (c *Collection) UpdateMany(ctx context.Context, filter, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	mongoOpts := options.UpdateMany()

	result, err := c.coll.UpdateMany(ctx, filter, update, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: update many: %w", err)
	}
	return &UpdateResult{
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		UpsertedID:    result.UpsertedID,
	}, nil
}

// UpdateResult represents the result of an Update operation.
type UpdateResult struct {
	MatchedCount  int64
	ModifiedCount int64
	UpsertedID    interface{}
}

// DeleteOne deletes a single document matching the filter.
func (c *Collection) DeleteOne(ctx context.Context, filter interface{}, opts ...*DeleteOptions) (*DeleteResult, error) {
	mongoOpts := options.DeleteOne()

	result, err := c.coll.DeleteOne(ctx, filter, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: delete one: %w", err)
	}
	return &DeleteResult{DeletedCount: result.DeletedCount}, nil
}

// DeleteMany deletes multiple documents matching the filter.
func (c *Collection) DeleteMany(ctx context.Context, filter interface{}, opts ...*DeleteOptions) (*DeleteResult, error) {
	mongoOpts := options.DeleteMany()

	result, err := c.coll.DeleteMany(ctx, filter, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: delete many: %w", err)
	}
	return &DeleteResult{DeletedCount: result.DeletedCount}, nil
}

// DeleteResult represents the result of a Delete operation.
type DeleteResult struct {
	DeletedCount int64
}

// ReplaceOne replaces a single document matching the filter with the replacement.
func (c *Collection) ReplaceOne(ctx context.Context, filter, replacement interface{}, opts ...*ReplaceOptions) (*UpdateResult, error) {
	mongoOpts := options.Replace()

	result, err := c.coll.ReplaceOne(ctx, filter, replacement, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: replace one: %w", err)
	}
	return &UpdateResult{
		MatchedCount:  result.MatchedCount,
		ModifiedCount: result.ModifiedCount,
		UpsertedID:    result.UpsertedID,
	}, nil
}

// Aggregate runs an aggregation pipeline and returns a cursor over the results.
func (c *Collection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*AggregateOptions) (*Cursor, error) {
	mongoOpts := options.Aggregate()

	cursor, err := c.coll.Aggregate(ctx, pipeline, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: aggregate: %w", err)
	}
	return &Cursor{cur: cursor}, nil
}

// Distinct returns distinct values for a specified field across a single collection.
func (c *Collection) Distinct(ctx context.Context, fieldName string, filter interface{}, opts ...*DistinctOptions) ([]interface{}, error) {
	mongoOpts := options.Distinct()

	result, err := c.coll.Distinct(ctx, fieldName, filter, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: distinct: %w", err)
	}
	return result, nil
}

// CountDocuments returns the number of documents matching the filter.
func (c *Collection) CountDocuments(ctx context.Context, filter interface{}, opts ...*CountOptions) (int64, error) {
	mongoOpts := options.Count()

	count, err := c.coll.CountDocuments(ctx, filter, mongoOpts)
	if err != nil {
		return 0, fmt.Errorf("mdb: count documents: %w", err)
	}
	return count, nil
}

// CreateIndex creates an index with the given keys and options.
func (c *Collection) CreateIndex(ctx context.Context, keys interface{}, opts ...*IndexOptions) (string, error) {
	mongoOpts := options.Index()
	if len(opts) > 0 && opts[0] != nil {
		o := opts[0]
		mongoOpts.SetUnique(o.Unique)
		mongoOpts.SetSparse(o.Sparse)
		mongoOpts.SetBackground(o.Background)
		if o.ExpireAfterSeconds != nil {
			mongoOpts.SetExpireAfterSeconds(*o.ExpireAfterSeconds)
		}
		if o.Name != "" {
			mongoOpts.SetName(o.Name)
		}
	}

	name, err := c.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    keys,
		Options: mongoOpts,
	})
	if err != nil {
		return "", fmt.Errorf("mdb: create index: %w", err)
	}
	return name, nil
}

// DropIndex drops the index with the given name.
func (c *Collection) DropIndex(ctx context.Context, name string) error {
	_, err := c.coll.Indexes().DropOne(ctx, name)
	if err != nil {
		return fmt.Errorf("mdb: drop index %q: %w", name, err)
	}
	return nil
}

// ListIndexes returns a cursor over all indexes on the collection.
func (c *Collection) ListIndexes(ctx context.Context) (*Cursor, error) {
	cursor, err := c.coll.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("mdb: list indexes: %w", err)
	}
	return &Cursor{cur: cursor}, nil
}

// Watch starts a change stream for the collection.
func (c *Collection) Watch(ctx context.Context, pipeline interface{}, opts ...*WatchOptions) (*ChangeStream, error) {
	mongoOpts := options.ChangeStream()

	cs, err := c.coll.Watch(ctx, pipeline, mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: watch: %w", err)
	}
	return &ChangeStream{cs: cs}, nil
}

// GridFSBucket returns a GridFS bucket for the collection.
func (c *Collection) GridFSBucket(opts ...*GridFSBucketOptions) *GridFSBucket {
	var bucketOpts *options.GridFSBucketOptions
	if len(opts) > 0 && opts[0] != nil {
		bucketOpts = options.GridFSBucket()
		if opts[0].Name != "" {
			bucketOpts.SetName(opts[0].Name)
		}
		if opts[0].ChunkSize != 0 {
			bucketOpts.SetChunkSizeBytes(opts[0].ChunkSize)
		}
	}

	// Use the parent database's GridFS bucket
	db := c.coll.Database()
	bucket, err := db.GridFSBucket(bucketOpts)
	if err != nil {
		// Fallback: return nil on error
		return nil
	}
	return &GridFSBucket{bucket: bucket}
}

// Stats returns statistics about the collection.
func (c *Collection) Stats(ctx context.Context) (bson.M, error) {
	var result bson.M
	err := c.coll.Database().RunCommand(ctx, bson.D{
		{Key: "collStats", Value: c.coll.Name()},
	}).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("mdb: collection stats: %w", err)
	}
	return result, nil
}

// Validate validates the collection's data and indexes.
func (c *Collection) Validate(ctx context.Context) (bson.M, error) {
	var result bson.M
	err := c.coll.Database().RunCommand(ctx, bson.D{
		{Key: "validate", Value: c.coll.Name()},
	}).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("mdb: validate collection: %w", err)
	}
	return result, nil
}

// Compact compacts the collection, reclaiming unused space.
func (c *Collection) Compact(ctx context.Context) error {
	err := c.coll.Database().RunCommand(ctx, bson.D{
		{Key: "compact", Value: c.coll.Name()},
	}).Err()
	if err != nil {
		return fmt.Errorf("mdb: compact collection: %w", err)
	}
	return nil
}

// ReIndex rebuilds all indexes for the collection.
func (c *Collection) ReIndex(ctx context.Context) error {
	err := c.coll.Database().RunCommand(ctx, bson.D{
		{Key: "reIndex", Value: c.coll.Name()},
	}).Err()
	if err != nil {
		return fmt.Errorf("mdb: reindex collection: %w", err)
	}
	return nil
}
