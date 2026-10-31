package mdb

import (
	"fmt"
	"time"
)

// GridFSBucket provides GridFS file storage operations.
type GridFSBucket struct {
	db         *MdbDatabase
	bucketName string
}

// NewGridFSBucket creates a new GridFSBucket.
func NewGridFSBucket(db *MdbDatabase, bucketName string) *GridFSBucket {
	if bucketName == "" {
		bucketName = "fs"
	}
	return &GridFSBucket{
		db:         db,
		bucketName: bucketName,
	}
}

// Upload stores a file in GridFS. Returns the file ID as a hex string.
func (b *GridFSBucket) Upload(filename string, data []byte) (string, error) {
	chunkSize := int32(255 * 1024) // 255KB default chunk size
	fileID := NewObjectID()

	// Insert chunks
	chunksColl := b.db.Collection(b.bucketName + ".chunks")
	numChunks := (len(data) + int(chunkSize) - 1) / int(chunkSize)
	for i := 0; i < numChunks; i++ {
		start := i * int(chunkSize)
		end := start + int(chunkSize)
		if end > len(data) {
			end = len(data)
		}
		chunk := map[string]interface{}{
			"files_id": fileID,
			"n":        int32(i),
			"data":     Binary{Data: data[start:end], SubType: 0},
		}
		_, err := chunksColl.InsertOne(chunk)
		if err != nil {
			return "", fmt.Errorf("gridfs: insert chunk: %w", err)
		}
	}

	// Insert files metadata
	filesColl := b.db.Collection(b.bucketName + ".files")
	fileDoc := map[string]interface{}{
		"_id":        fileID,
		"length":     int64(len(data)),
		"chunkSize":  chunkSize,
		"filename":   filename,
		"uploadDate": time.Now(),
	}
	_, err := filesColl.InsertOne(fileDoc)
	if err != nil {
		return "", fmt.Errorf("gridfs: insert file: %w", err)
	}

	return fileID.String(), nil
}

// Download retrieves a file from GridFS by its ID.
func (b *GridFSBucket) Download(fileId string) ([]byte, error) {
	// Find the file metadata
	filesColl := b.db.Collection(b.bucketName + ".files")
	file, err := filesColl.FindOne(map[string]interface{}{
		"_id": fileId,
	})
	if err != nil {
		return nil, fmt.Errorf("gridfs: find file: %w", err)
	}
	if file == nil {
		return nil, fmt.Errorf("gridfs: file not found: %s", fileId)
	}

	length, _ := file["length"].(int64)
	chunkSize, _ := file["chunkSize"].(int32)
	if chunkSize == 0 {
		chunkSize = 255 * 1024
	}

	// Read all chunks
	var result []byte
	chunksColl := b.db.Collection(b.bucketName + ".chunks")
	numChunks := int((length + int64(chunkSize) - 1) / int64(chunkSize))

	for i := 0; i < numChunks; i++ {
		chunk, err := chunksColl.FindOne(map[string]interface{}{
			"files_id": fileId,
			"n":        int32(i),
		})
		if err != nil {
			return nil, fmt.Errorf("gridfs: read chunk %d: %w", i, err)
		}
		if chunk == nil {
			return nil, fmt.Errorf("gridfs: missing chunk %d", i)
		}
		binData, ok := chunk["data"].(Binary)
		if !ok {
			return nil, fmt.Errorf("gridfs: invalid chunk data type")
		}
		result = append(result, binData.Data...)
	}

	return result, nil
}

// ListFiles returns all files in the GridFS bucket.
func (b *GridFSBucket) ListFiles() ([]map[string]interface{}, error) {
	filesColl := b.db.Collection(b.bucketName + ".files")
	cursor := filesColl.Find(map[string]interface{}{})
	return cursor.All()
}

// Delete removes a file and its chunks from GridFS.
func (b *GridFSBucket) Delete(fileId string) error {
	// Delete chunks
	chunksColl := b.db.Collection(b.bucketName + ".chunks")
	_, err := chunksColl.DeleteMany(map[string]interface{}{
		"files_id": fileId,
	})
	if err != nil {
		return fmt.Errorf("gridfs: delete chunks: %w", err)
	}

	// Delete file metadata
	filesColl := b.db.Collection(b.bucketName + ".files")
	_, err = filesColl.DeleteOne(map[string]interface{}{
		"_id": fileId,
	})
	if err != nil {
		return fmt.Errorf("gridfs: delete file: %w", err)
	}

	return nil
}
