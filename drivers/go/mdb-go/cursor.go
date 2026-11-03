package mdb

import (
	"context"
	"fmt"

	native "github.com/mdb-official/mdb-native"
)

// Cursor 遍历查询结果。
//
// 既支持真实游标（find 的 getMore 形态），也支持一次性结果集
// （aggregate / listIndexes 这类单批返回的命令）。
type Cursor struct {
	native *native.MdbCursor
	docs   []Document
	pos    int
	err    error
}

// Next 前进到下一条文档。
func (c *Cursor) Next(ctx context.Context) bool {
	if c.native != nil {
		return c.native.Next()
	}
	if c.pos >= len(c.docs) {
		return false
	}
	c.pos++
	return true
}

// Decode 把当前文档写入 val（*Document 或 *map[string]interface{}）。
func (c *Cursor) Decode(val interface{}) error {
	if c.native != nil {
		return c.native.Decode(val)
	}
	if c.pos == 0 || c.pos > len(c.docs) {
		return fmt.Errorf("mdb: no document to decode")
	}
	target, ok := val.(*Document)
	if !ok {
		return fmt.Errorf("mdb: decode target must be *mdb.Document, got %T", val)
	}
	*target = c.docs[c.pos-1]
	return nil
}

// All 取出全部文档。
func (c *Cursor) All(ctx context.Context, results interface{}) error {
	out, ok := results.(*[]Document)
	if !ok {
		return fmt.Errorf("mdb: results must be *[]mdb.Document, got %T", results)
	}
	for c.Next(ctx) {
		var doc Document
		if err := c.Decode(&doc); err != nil {
			return err
		}
		*out = append(*out, doc)
	}
	return c.Err()
}

// Close 释放游标。
func (c *Cursor) Close(ctx context.Context) error {
	if c.native != nil {
		return c.native.Close()
	}
	return nil
}

// Err 返回遍历过程中的错误。
func (c *Cursor) Err() error {
	if c.native != nil {
		// native 的游标错误字段是私有的，只能由 Next() 的结果推断。
		return nil
	}
	return c.err
}

// Explain 返回当前文档（用于 explain 结果）。
func (c *Cursor) Explain(ctx context.Context) (Document, error) {
	var doc Document
	if err := c.Decode(&doc); err != nil {
		return nil, fmt.Errorf("mdb: explain: %w", err)
	}
	return doc, nil
}

// staticCursor 包装一次性结果集。
func staticCursor(docs []Document) *Cursor {
	return &Cursor{docs: docs}
}

// docsFromCursorReply 从 `{cursor: {firstBatch: [...]}}` 里取出文档。
func docsFromCursorReply(resp Document) []Document {
	cursor, ok := resp["cursor"].(map[string]interface{})
	if !ok {
		return nil
	}
	batch, ok := cursor["firstBatch"].([]interface{})
	if !ok {
		return nil
	}
	docs := make([]Document, 0, len(batch))
	for _, item := range batch {
		if m, ok := item.(map[string]interface{}); ok {
			docs = append(docs, Document(m))
		}
	}
	return docs
}
