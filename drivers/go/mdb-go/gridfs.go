package mdb

import (
	"context"
	"fmt"
	"io"

	native "github.com/mdb-official/mdb-native"
)

// GridFSBucket 提供 GridFS 文件操作。
type GridFSBucket struct {
	bucket *native.GridFSBucket
	db     *Database
}

// GridFSFile 是一个 GridFS 文件的元信息。
type GridFSFile struct {
	ID         string
	Length     int64
	ChunkSize  int32
	UploadDate int64
	Filename   string
	Metadata   Document
}

// GridFSBucket 返回该库上的 GridFS bucket。
func (d *Database) GridFSBucket(opts ...*GridFSBucketOptions) *GridFSBucket {
	name := "fs"
	if len(opts) > 0 && opts[0] != nil && opts[0].Name != "" {
		name = opts[0].Name
	}
	return &GridFSBucket{bucket: native.NewGridFSBucket(d.db, name), db: d}
}

// UploadFromBytes 上传字节切片。
func (b *GridFSBucket) UploadFromBytes(filename string, data []byte) (string, error) {
	id, err := b.bucket.Upload(filename, data)
	if err != nil {
		return "", fmt.Errorf("mdb: gridfs upload: %w", err)
	}
	return id, nil
}

// UploadFromStream 上传 io.Reader 的全部内容。
func (b *GridFSBucket) UploadFromStream(filename string, reader io.Reader) (string, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return "", fmt.Errorf("mdb: gridfs read: %w", err)
	}
	return b.UploadFromBytes(filename, data)
}

// DownloadToBytes 下载为字节切片。
func (b *GridFSBucket) DownloadToBytes(fileID string) ([]byte, error) {
	data, err := b.bucket.Download(fileID)
	if err != nil {
		return nil, fmt.Errorf("mdb: gridfs download: %w", err)
	}
	return data, nil
}

// DownloadToStream 下载到 io.Writer。
func (b *GridFSBucket) DownloadToStream(fileID string, writer io.Writer) error {
	data, err := b.DownloadToBytes(fileID)
	if err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("mdb: gridfs write: %w", err)
	}
	return nil
}

// Find 查询文件元信息。
func (b *GridFSBucket) Find(ctx context.Context, filter interface{}) ([]GridFSFile, error) {
	files, err := b.bucket.ListFiles()
	if err != nil {
		return nil, fmt.Errorf("mdb: gridfs find: %w", err)
	}
	f, err := toDocument(filter)
	if err != nil {
		return nil, err
	}
	out := make([]GridFSFile, 0, len(files))
	for _, raw := range files {
		file := gridfsFileFromDocument(raw)
		if matchesDocument(raw, f) {
			out = append(out, file)
		}
	}
	return out, nil
}

// Delete 删除文件。
func (b *GridFSBucket) Delete(fileID string) error {
	if err := b.bucket.Delete(fileID); err != nil {
		return fmt.Errorf("mdb: gridfs delete: %w", err)
	}
	return nil
}

// Rename 重命名文件。
func (b *GridFSBucket) Rename(fileID string, newFilename string) error {
	resp, err := b.db.db.Command(map[string]interface{}{
		"findAndModify": b.bucketName() + ".files",
		"query":         map[string]interface{}{"_id": fileID},
		"update":        map[string]interface{}{"$set": map[string]interface{}{"filename": newFilename}},
	})
	if err != nil {
		return fmt.Errorf("mdb: gridfs rename: %w", err)
	}
	return commandErr(resp)
}

// bucketName 返回 bucket 使用的集合前缀。
func (b *GridFSBucket) bucketName() string {
	return "fs"
}

func gridfsFileFromDocument(doc Document) GridFSFile {
	file := GridFSFile{}
	if id, ok := doc["_id"].(string); ok {
		file.ID = id
	} else {
		file.ID = fmt.Sprintf("%v", doc["_id"])
	}
	if name, ok := doc["filename"].(string); ok {
		file.Filename = name
	}
	if n, ok := doc["length"].(float64); ok {
		file.Length = int64(n)
	}
	if n, ok := doc["chunkSize"].(float64); ok {
		file.ChunkSize = int32(n)
	}
	if n, ok := doc["uploadDate"].(float64); ok {
		file.UploadDate = int64(n)
	}
	if m, ok := doc["metadata"].(map[string]interface{}); ok {
		file.Metadata = Document(m)
	}
	return file
}

// matchesDocument 做最朴素的等值过滤（GridFS find 的常用场景）。
func matchesDocument(doc, filter Document) bool {
	for k, v := range filter {
		if fmt.Sprintf("%v", doc[k]) != fmt.Sprintf("%v", v) {
			return false
		}
	}
	return true
}
