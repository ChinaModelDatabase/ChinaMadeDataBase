package mdb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ChangeStream wraps the official mongo-go-driver change stream with MDB-specific extensions.
type ChangeStream struct {
	cs *mongo.ChangeStream
}

// Next returns true if there is a change event available to decode.
func (cs *ChangeStream) Next(ctx context.Context) bool {
	return cs.cs.Next(ctx)
}

// Decode decodes the current change event into the provided value.
func (cs *ChangeStream) Decode(val interface{}) error {
	return cs.cs.Decode(val)
}

// ResumeAfter sets the resume token to resume after the specified token.
func (cs *ChangeStream) ResumeAfter(token interface{}) *ChangeStream {
	opts := options.ChangeStream().SetResumeAfter(token)
	cs.cs.SetOptions(opts)
	return cs
}

// StartAfter sets the resume token to start after the specified token.
func (cs *ChangeStream) StartAfter(token interface{}) *ChangeStream {
	opts := options.ChangeStream().SetStartAfter(token)
	cs.cs.SetOptions(opts)
	return cs
}

// StartAtOperationTime sets the operation time to start the change stream from.
func (cs *ChangeStream) StartAtOperationTime(t primitive.Timestamp) *ChangeStream {
	opts := options.ChangeStream().SetStartAtOperationTime(t)
	cs.cs.SetOptions(opts)
	return cs
}

// Close closes the change stream and releases associated resources.
func (cs *ChangeStream) Close(ctx context.Context) error {
	return cs.cs.Close(ctx)
}

// Err returns the last error encountered by the change stream.
func (cs *ChangeStream) Err() error {
	return cs.cs.Err()
}

// ID returns the change stream's ID.
func (cs *ChangeStream) ID() interface{} {
	return cs.cs.ID()
}

// ResumeToken returns the most recent resume token.
func (cs *ChangeStream) ResumeToken() interface{} {
	return cs.cs.ResumeToken()
}

// SetOptions applies additional options to the change stream.
func (cs *ChangeStream) SetOptions(opts *options.ChangeStreamOptions) {
	cs.cs.SetOptions(opts)
}

// TryNext attempts to get the next change event without blocking.
func (cs *ChangeStream) TryNext(ctx context.Context) bool {
	return cs.cs.TryNext(ctx)
}
