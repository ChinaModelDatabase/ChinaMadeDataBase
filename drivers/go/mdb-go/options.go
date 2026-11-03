package mdb

import "time"

// ClientOptions holds configuration for a Client.
type ClientOptions struct {
	WriteConcern WriteConcern
	ReadConcern  ReadConcern
	AppName      string
	MaxPoolSize  uint64
	MinPoolSize  uint64
	Timeout      time.Duration
	// Database 是连接串里带的默认库名（事务/watch 需要它来拼命名空间）。
	Database string
}

// ClientOption is a functional option for configuring a Client.
type ClientOption func(*ClientOptions)

// WithWriteConcern sets the write concern for the client.
func WithWriteConcern(wc WriteConcern) ClientOption {
	return func(o *ClientOptions) {
		o.WriteConcern = wc
	}
}

// WithReadConcern sets the read concern for the client.
func WithReadConcern(rc ReadConcern) ClientOption {
	return func(o *ClientOptions) {
		o.ReadConcern = rc
	}
}

// WithAppName sets the application name for the client.
func WithAppName(name string) ClientOption {
	return func(o *ClientOptions) {
		o.AppName = name
	}
}

// WithMaxPoolSize sets the maximum number of connections in the connection pool.
func WithMaxPoolSize(n uint64) ClientOption {
	return func(o *ClientOptions) {
		o.MaxPoolSize = n
	}
}

// WithMinPoolSize sets the minimum number of connections in the connection pool.
func WithMinPoolSize(n uint64) ClientOption {
	return func(o *ClientOptions) {
		o.MinPoolSize = n
	}
}

// WithTimeout sets the connection timeout for the client.
func WithTimeout(d time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.Timeout = d
	}
}

func applyOptions(opts []ClientOption) *ClientOptions {
	o := &ClientOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// CreateCollectionOptions configures collection creation behavior.
type CreateCollectionOptions struct {
	Capped          bool
	SizeInBytes     int64
	MaxDocuments    int64
	ValidationLevel string
}

// FindOptions configures find operations.
type FindOptions struct {
	Limit      int64
	Skip       int64
	Sort       interface{}
	Projection interface{}
}

// FindOneOptions configures find-one operations.
type FindOneOptions struct {
	Projection interface{}
	Sort       interface{}
}

// InsertOneOptions configures insert-one operations.
type InsertOneOptions struct{}

// InsertManyOptions configures insert-many operations.
type InsertManyOptions struct {
	Ordered bool
}

// UpdateOptions configures update operations.
type UpdateOptions struct {
	// Upsert 为 true 时：没有匹配文档就按过滤条件插入一条。
	Upsert bool
}

// DeleteOptions configures delete operations.
type DeleteOptions struct{}

// ReplaceOptions configures replace operations.
type ReplaceOptions struct {
	Upsert bool
}

// AggregateOptions configures aggregation pipeline operations.
type AggregateOptions struct{}

// DistinctOptions configures distinct operations.
type DistinctOptions struct{}

// CountOptions configures count operations.
type CountOptions struct{}

// IndexOptions configures index creation.
type IndexOptions struct {
	Unique             bool
	Sparse             bool
	ExpireAfterSeconds *int32
	Name               string
	Background         bool
}

// WatchOptions configures change stream watch operations.
type WatchOptions struct{}

// GridFSBucketOptions configures a GridFS bucket.
type GridFSBucketOptions struct {
	Name      string
	ChunkSize int32
}

// TransactionOptions configures transactions.
type TransactionOptions struct {
	ReadConcern  *ReadConcern
	WriteConcern *WriteConcern
}

// SessionOptions configures sessions.
type SessionOptions struct{}
