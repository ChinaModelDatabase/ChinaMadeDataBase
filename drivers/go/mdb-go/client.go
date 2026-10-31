package mdb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Client wraps the official mongo-go-driver client with MDB-specific extensions.
// It provides methods for database operations, server status, and replica set management.
type Client struct {
	client *mongo.Client
	opts   *ClientOptions
}

// NewClient creates a new MDB client connecting to the specified host and port.
func NewClient(host string, port int, opts ...ClientOption) (*Client, error) {
	o := applyOptions(opts)
	uri := fmt.Sprintf("mongodb://%s:%d", host, port)

	mongoOpts := options.Client().ApplyURI(uri)
	if o.AppName != "" {
		mongoOpts.SetAppName(o.AppName)
	}
	if o.MaxPoolSize > 0 {
		mongoOpts.SetMaxPoolSize(o.MaxPoolSize)
	}
	if o.MinPoolSize > 0 {
		mongoOpts.SetMinPoolSize(o.MinPoolSize)
	}
	if o.Timeout > 0 {
		mongoOpts.SetConnectTimeout(o.Timeout)
	}

	client, err := mongo.Connect(context.TODO(), mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: failed to connect: %w", err)
	}

	return &Client{client: client, opts: o}, nil
}

// NewClientFromURI creates a new MDB client from a MongoDB connection URI.
func NewClientFromURI(uri string, opts ...ClientOption) (*Client, error) {
	o := applyOptions(opts)

	mongoOpts := options.Client().ApplyURI(uri)
	if o.AppName != "" {
		mongoOpts.SetAppName(o.AppName)
	}
	if o.MaxPoolSize > 0 {
		mongoOpts.SetMaxPoolSize(o.MaxPoolSize)
	}
	if o.MinPoolSize > 0 {
		mongoOpts.SetMinPoolSize(o.MinPoolSize)
	}
	if o.Timeout > 0 {
		mongoOpts.SetConnectTimeout(o.Timeout)
	}

	client, err := mongo.Connect(context.TODO(), mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: failed to connect: %w", err)
	}

	return &Client{client: client, opts: o}, nil
}

// NewClientWithAuth creates a new MDB client with authentication credentials.
func NewClientWithAuth(host, port, username, password, authDB string) (*Client, error) {
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%d/%s",
		username, password, host, port, authDB)

	mongoOpts := options.Client().ApplyURI(uri)

	client, err := mongo.Connect(context.TODO(), mongoOpts)
	if err != nil {
		return nil, fmt.Errorf("mdb: failed to connect with auth: %w", err)
	}

	return &Client{client: client, opts: &ClientOptions{}}, nil
}

// Database returns a Database handle for the given database name.
func (c *Client) Database(name string) *Database {
	return &Database{db: c.client.Database(name)}
}

// ListDatabases returns the names of all databases on the server.
func (c *Client) ListDatabases(ctx context.Context) ([]string, error) {
	result, err := c.client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("mdb: list databases: %w", err)
	}
	return result, nil
}

// ServerStatus returns the server status information from the MDB instance.
func (c *Client) ServerStatus(ctx context.Context) (bson.M, error) {
	var result bson.M
	err := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("mdb: server status: %w", err)
	}
	return result, nil
}

// ReplicaSetStatus returns the replica set status information.
func (c *Client) ReplicaSetStatus(ctx context.Context) (bson.M, error) {
	var result bson.M
	err := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("mdb: replica set status: %w", err)
	}
	return result, nil
}

// Ping verifies the connection to the server is alive.
func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx, readpref.Primary())
}

// IsConnected returns true if the client has an active connection to the server.
func (c *Client) IsConnected() bool {
	return c.client.Ping(context.Background(), readpref.Primary()) == nil
}

// Close disconnects from the server and releases associated resources.
func (c *Client) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

// StartSession starts a new client session.
func (c *Client) StartSession(opts ...SessionOptions) (mongo.Session, error) {
	return c.client.StartSession()
}
