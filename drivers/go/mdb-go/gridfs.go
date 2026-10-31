package mdb

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
)

// GridFSBucket wraps the official mongo-go-driver GridFS bucket with MDB-specific extensions.
type GridFSBucket struct {
	bucket *gridfs.Bucket
}

// GridFSFile represents metadata about a file stored in GridFS.
type GridFSFile struct {
	ID           primitive.ObjectID `bson:"_id"`
	Length       int64              `bson:"length"`
	ChunkSize    int32              `bson:"chunkSize"`
	UploadDate   primitive.DateTime `bson:"uploadDate"`
	Filename     string             `bson:"filename"`
	Metadata     bson.M             `bson:"metadata,omitempty"`
}

// UploadFromBytes uploads a byte slice to GridFS and returns the file ID.
func (b *GridFSBucket) UploadFromBytes(filename string, data []byte) (primitive.ObjectID, error) {
	reader := bytes.NewReader(data)
	return b.UploadFromStream(filename, reader)
}

// UploadFromStream uploads data from an io.Reader to GridFS and returns the file ID.
func (b *GridFSBucket) UploadFromStream(filename string, reader io.Reader) (primitive.ObjectID, error) {
	fileID, err := b.bucket.UploadFromStream(filename, reader)
	if err != nil {
		return primitive.NilObjectID, fmt.Errorf("mdb: gridfs upload: %w", err)
	}
	return fileID, nil
}

// DownloadToBytes downloads a file from GridFS into a byte slice.
func (b *GridFSBucket) DownloadToBytes(fileID primitive.ObjectID) ([]byte, error) {
	var buf bytes.Buffer
	_, err := b.bucket.DownloadToStream(fileID, &buf)
	if err != nil {
		return nil, fmt.Errorf("mdb: gridfs download: %w", err)
	}
	return buf.Bytes(), nil
}

// DownloadToStream downloads a file from GridFS to an io.Writer.
func (b *GridFSBucket) DownloadToStream(fileID primitive.ObjectID, writer io.Writer) error {
	_, err := b.bucket.DownloadToStream(fileID, writer)
	if err != nil {
		return fmt.Errorf("mdb: gridfs download stream: %w", err)
	}
	return nil
}

// Find searches for files matching the given filter.
func (b *GridFSBucket) Find(filter interface{}) ([]GridFSFile, error) {
	ctx := context.Background()
	cursor, err := b.bucket.Find(filter)
	if err != nil {
		return nil, fmt.Errorf("mdb: gridfs find: %w", err)
	}
	defer cursor.Close(ctx)

	var files []GridFSFile
	for cursor.Next(ctx) {
		var file GridFSFile
		if err := cursor.Decode(&file); err != nil {
			return nil, fmt.Errorf("mdb: gridfs decode file: %w", err)
		}
		files = append(files, file)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mdb: gridfs cursor: %w", err)
	}
	return files, nil
}

// Delete deletes a file from GridFS by its ID.
func (b *GridFSBucket) Delete(fileID primitive.ObjectID) error {
	err := b.bucket.Delete(fileID)
	if err != nil {
		return fmt.Errorf("mdb: gridfs delete: %w", err)
	}
	return nil
}

// Rename renames a file in GridFS.
func (b *GridFSBucket) Rename(fileID primitive.ObjectID, newFilename string) error {
	err := b.bucket.Rename(fileID, newFilename)
	if err != nil {
		return fmt.Errorf("mdb: gridfs rename: %w", err)
	}
	return nil
}
