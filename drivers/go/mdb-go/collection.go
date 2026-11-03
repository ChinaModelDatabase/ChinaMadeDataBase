package mdb

import (
	"context"
	"fmt"

	native "github.com/mdb-official/mdb-native"
)

// Collection 是集合句柄。
//
// 写入操作在事务中会携带 lsid / txnNumber / autocommit，因此这里统一
// 走 `Command`（而不是 native 的便捷方法），保证事务语义不被丢掉。
type Collection struct {
	coll *native.MdbCollection
	db   *Database
	txn  *Transaction
}

// Name 返回集合名。
func (c *Collection) Name() string { return c.coll.Name() }

// Find 查询并返回游标。
func (c *Collection) Find(ctx context.Context, filter interface{}, opts ...*FindOptions) (*Cursor, error) {
	f, err := toDocument(filter)
	if err != nil {
		return nil, err
	}
	cur := c.coll.Find(f)
	if len(opts) > 0 && opts[0] != nil {
		o := opts[0]
		if o.Limit != 0 {
			cur = cur.Limit(int(o.Limit))
		}
		if o.Skip != 0 {
			cur = cur.Skip(int(o.Skip))
		}
		if o.Sort != nil {
			sort, err := toDocument(o.Sort)
			if err != nil {
				return nil, err
			}
			cur = cur.Sort(sort)
		}
		if o.Projection != nil {
			proj, err := toDocument(o.Projection)
			if err != nil {
				return nil, err
			}
			cur = cur.Projection(proj)
		}
	}
	return &Cursor{native: cur}, nil
}

// SingleResult 是 FindOne 的结果。
type SingleResult struct {
	doc Document
	err error
}

// Decode 把结果写入 val。
func (sr *SingleResult) Decode(val interface{}) error {
	if sr.err != nil {
		return sr.err
	}
	target, ok := val.(*Document)
	if !ok {
		return fmt.Errorf("mdb: decode target must be *mdb.Document, got %T", val)
	}
	*target = sr.doc
	return nil
}

// Err 返回错误（含"没有匹配文档"）。
func (sr *SingleResult) Err() error { return sr.err }

// FindOne 查询单条文档。
func (c *Collection) FindOne(ctx context.Context, filter interface{}, opts ...*FindOneOptions) (*SingleResult, error) {
	f, err := toDocument(filter)
	if err != nil {
		return nil, err
	}
	if len(opts) > 0 && opts[0] != nil && opts[0].Sort != nil {
		// 底层 FindOne 不支持 sort：退化成 limit 1 的查询。
		cur := c.coll.Find(f)
		sort, err := toDocument(opts[0].Sort)
		if err != nil {
			return nil, err
		}
		cur = cur.Sort(sort).Limit(1)
		if !cur.Next() {
			return &SingleResult{err: fmt.Errorf("mdb: no documents in result")}, nil
		}
		var doc Document
		if err := cur.Decode(&doc); err != nil {
			return nil, fmt.Errorf("mdb: find one: %w", err)
		}
		return &SingleResult{doc: doc}, nil
	}

	doc, err := c.coll.FindOne(f)
	if err != nil {
		return nil, fmt.Errorf("mdb: find one: %w", err)
	}
	if doc == nil {
		return &SingleResult{err: fmt.Errorf("mdb: no documents in result")}, nil
	}
	return &SingleResult{doc: Document(doc)}, nil
}

// InsertOneResult 是 InsertOne 的结果。
type InsertOneResult struct {
	InsertedID interface{}
}

// InsertOne 插入一条文档；缺 `_id` 时客户端补一个。
func (c *Collection) InsertOne(ctx context.Context, doc interface{}, opts ...*InsertOneOptions) (*InsertOneResult, error) {
	d, err := toDocument(doc)
	if err != nil {
		return nil, err
	}
	if _, ok := d["_id"]; !ok {
		d["_id"] = ObjectID()
	}
	resp, err := c.runWrite(map[string]interface{}{
		"insert":    c.coll.Name(),
		"documents": []interface{}{d},
	})
	if err != nil {
		return nil, fmt.Errorf("mdb: insert one: %w", err)
	}
	if err := writeErrors(resp); err != nil {
		return nil, err
	}
	return &InsertOneResult{InsertedID: d["_id"]}, nil
}

// InsertManyResult 是 InsertMany 的结果。
type InsertManyResult struct {
	InsertedIDs []interface{}
}

// InsertMany 插入多条文档。
func (c *Collection) InsertMany(ctx context.Context, docs []interface{}, opts ...*InsertManyOptions) (*InsertManyResult, error) {
	list, err := toDocumentSlice(docs)
	if err != nil {
		return nil, err
	}
	ordered := true
	if len(opts) > 0 && opts[0] != nil {
		ordered = opts[0].Ordered
	}
	for _, d := range list {
		if _, ok := d["_id"]; !ok {
			d["_id"] = ObjectID()
		}
	}
	arr := make([]interface{}, len(list))
	for i, d := range list {
		arr[i] = d
	}
	resp, err := c.runWrite(map[string]interface{}{
		"insert":    c.coll.Name(),
		"ordered":   ordered,
		"documents": arr,
	})
	if err != nil {
		return nil, fmt.Errorf("mdb: insert many: %w", err)
	}
	if err := writeErrors(resp); err != nil {
		return nil, err
	}
	ids := make([]interface{}, 0, len(list))
	for _, d := range list {
		ids = append(ids, d["_id"])
	}
	return &InsertManyResult{InsertedIDs: ids}, nil
}

// UpdateResult 是更新操作的结果。
type UpdateResult struct {
	MatchedCount  int64
	ModifiedCount int64
	UpsertedID    interface{}
}

// UpdateOne 更新单条文档。
func (c *Collection) UpdateOne(ctx context.Context, filter, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	return c.update(ctx, filter, update, false, opts...)
}

// UpdateMany 更新所有匹配文档。
func (c *Collection) UpdateMany(ctx context.Context, filter, update interface{}, opts ...*UpdateOptions) (*UpdateResult, error) {
	return c.update(ctx, filter, update, true, opts...)
}

// ReplaceOne 用替换文档覆盖单条文档。
func (c *Collection) ReplaceOne(ctx context.Context, filter, replacement interface{}, opts ...*ReplaceOptions) (*UpdateResult, error) {
	var upsert bool
	if len(opts) > 0 && opts[0] != nil {
		upsert = opts[0].Upsert
	}
	return c.update(ctx, filter, replacement, false, &UpdateOptions{Upsert: upsert})
}

func (c *Collection) update(ctx context.Context, filter, update interface{}, multi bool, opts ...*UpdateOptions) (*UpdateResult, error) {
	f, err := toDocument(filter)
	if err != nil {
		return nil, err
	}
	u, err := toDocument(update)
	if err != nil {
		return nil, err
	}
	var upsert bool
	if len(opts) > 0 && opts[0] != nil {
		upsert = opts[0].Upsert
	}
	resp, err := c.runWrite(map[string]interface{}{
		"update": c.coll.Name(),
		"updates": []interface{}{
			map[string]interface{}{
				"q":      f,
				"u":      u,
				"multi":  multi,
				"upsert": upsert,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("mdb: update: %w", err)
	}
	if err := writeErrors(resp); err != nil {
		return nil, err
	}
	return parseUpdateResult(resp), nil
}

// DeleteResult 是删除操作的结果。
type DeleteResult struct {
	DeletedCount int64
}

// DeleteOne 删除单条文档。
func (c *Collection) DeleteOne(ctx context.Context, filter interface{}, opts ...*DeleteOptions) (*DeleteResult, error) {
	return c.delete(ctx, filter, 1)
}

// DeleteMany 删除所有匹配文档。
func (c *Collection) DeleteMany(ctx context.Context, filter interface{}, opts ...*DeleteOptions) (*DeleteResult, error) {
	return c.delete(ctx, filter, 0)
}

func (c *Collection) delete(ctx context.Context, filter interface{}, limit int) (*DeleteResult, error) {
	f, err := toDocument(filter)
	if err != nil {
		return nil, err
	}
	resp, err := c.runWrite(map[string]interface{}{
		"delete": c.coll.Name(),
		"deletes": []interface{}{
			map[string]interface{}{"q": f, "limit": limit},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("mdb: delete: %w", err)
	}
	if err := writeErrors(resp); err != nil {
		return nil, err
	}
	n, _ := resp["n"].(float64)
	return &DeleteResult{DeletedCount: int64(n)}, nil
}

// Aggregate 执行聚合管道。
func (c *Collection) Aggregate(ctx context.Context, pipeline interface{}, opts ...*AggregateOptions) (*Cursor, error) {
	p, err := toPipeline(pipeline)
	if err != nil {
		return nil, err
	}
	arr := make([]interface{}, len(p))
	for i, stage := range p {
		arr[i] = stage
	}
	resp, err := c.run(map[string]interface{}{
		"aggregate": c.coll.Name(),
		"pipeline":  arr,
		"cursor":    map[string]interface{}{},
	})
	if err != nil {
		return nil, fmt.Errorf("mdb: aggregate: %w", err)
	}
	if err := commandErr(resp); err != nil {
		return nil, err
	}
	return staticCursor(docsFromCursorReply(resp)), nil
}

// Distinct 返回某字段的去重值。
func (c *Collection) Distinct(ctx context.Context, fieldName string, filter interface{}, opts ...*DistinctOptions) ([]interface{}, error) {
	f, err := toDocument(filter)
	if err != nil {
		return nil, err
	}
	resp, err := c.run(map[string]interface{}{
		"distinct": c.coll.Name(),
		"key":      fieldName,
		"query":    f,
	})
	if err != nil {
		return nil, fmt.Errorf("mdb: distinct: %w", err)
	}
	if err := commandErr(resp); err != nil {
		return nil, err
	}
	values, _ := resp["values"].([]interface{})
	return values, nil
}

// CountDocuments 统计匹配文档数。
func (c *Collection) CountDocuments(ctx context.Context, filter interface{}, opts ...*CountOptions) (int64, error) {
	f, err := toDocument(filter)
	if err != nil {
		return 0, err
	}
	resp, err := c.run(map[string]interface{}{
		"count": c.coll.Name(),
		"query": f,
	})
	if err != nil {
		return 0, fmt.Errorf("mdb: count documents: %w", err)
	}
	if err := commandErr(resp); err != nil {
		return 0, err
	}
	n, _ := resp["n"].(float64)
	return int64(n), nil
}

// CreateIndex 创建索引，返回索引名。
func (c *Collection) CreateIndex(ctx context.Context, keys interface{}, opts ...*IndexOptions) (string, error) {
	keyDoc, err := toDocument(keys)
	if err != nil {
		return "", err
	}
	spec := map[string]interface{}{"key": keyDoc}
	name := ""
	if len(opts) > 0 && opts[0] != nil {
		o := opts[0]
		if o.Name != "" {
			name = o.Name
		}
		spec["unique"] = o.Unique
		spec["sparse"] = o.Sparse
		spec["background"] = o.Background
		if o.ExpireAfterSeconds != nil {
			spec["expireAfterSeconds"] = *o.ExpireAfterSeconds
		}
	}
	if name == "" {
		name = defaultIndexName(keyDoc)
	}
	spec["name"] = name
	resp, err := c.run(map[string]interface{}{
		"createIndexes": c.coll.Name(),
		"indexes":       []interface{}{spec},
	})
	if err != nil {
		return "", fmt.Errorf("mdb: create index: %w", err)
	}
	if err := commandErr(resp); err != nil {
		return "", err
	}
	return name, nil
}

// defaultIndexName 按 MongoDB 惯例拼索引名：`field_dir_field_dir`。
func defaultIndexName(keys Document) string {
	parts := make([]string, 0, len(keys)*2)
	for k, v := range keys {
		parts = append(parts, fmt.Sprintf("%s_%v", k, v))
	}
	if len(parts) == 0 {
		return "idx"
	}
	return joinStrings(parts, "_")
}

// ListIndexes 列出集合上的索引。
func (c *Collection) ListIndexes(ctx context.Context) (*Cursor, error) {
	indexes, err := c.coll.ListIndexes()
	if err != nil {
		return nil, fmt.Errorf("mdb: list indexes: %w", err)
	}
	docs := make([]Document, 0, len(indexes))
	for _, idx := range indexes {
		docs = append(docs, Document(idx))
	}
	return staticCursor(docs), nil
}

// DropIndex 删除指定索引。
func (c *Collection) DropIndex(ctx context.Context, name string) error {
	if err := c.coll.DropIndex(name); err != nil {
		return fmt.Errorf("mdb: drop index %q: %w", name, err)
	}
	return nil
}

// Stats 返回 collStats 结果。
func (c *Collection) Stats(ctx context.Context) (Document, error) {
	return c.coll.Stats()
}

// Explain 返回查询计划。
func (c *Collection) Explain(ctx context.Context, filter interface{}) (Document, error) {
	f, err := toDocument(filter)
	if err != nil {
		return nil, err
	}
	return c.coll.Explain(f)
}

// Validate 校验集合。
func (c *Collection) Validate(ctx context.Context) (Document, error) {
	return c.run(map[string]interface{}{"validate": c.coll.Name()})
}

// Compact 压缩集合。
func (c *Collection) Compact(ctx context.Context) error {
	resp, err := c.run(map[string]interface{}{"compact": c.coll.Name()})
	if err != nil {
		return fmt.Errorf("mdb: compact collection: %w", err)
	}
	return commandErr(resp)
}

// ReIndex 重建索引。
func (c *Collection) ReIndex(ctx context.Context) error {
	resp, err := c.run(map[string]interface{}{"reIndex": c.coll.Name()})
	if err != nil {
		return fmt.Errorf("mdb: reindex collection: %w", err)
	}
	return commandErr(resp)
}

// GridFSBucket 返回该集合所在库的 GridFS bucket。
func (c *Collection) GridFSBucket(opts ...*GridFSBucketOptions) *GridFSBucket {
	return c.db.GridFSBucket(opts...)
}

// Watch 打开 change stream。
func (c *Collection) Watch(ctx context.Context, pipeline interface{}, opts ...*WatchOptions) (*ChangeStream, error) {
	return c.db.client.watch(ctx, c.coll.Name(), pipeline)
}

// run 发一条命令（事务中会自动带上会话字段）。
func (c *Collection) run(cmd map[string]interface{}) (Document, error) {
	if c.txn != nil {
		c.txn.apply(cmd)
	}
	return c.db.db.Command(cmd)
}

// runWrite 发一条写命令（事务里第一条写操作要带 startTransaction）。
func (c *Collection) runWrite(cmd map[string]interface{}) (Document, error) {
	return c.run(cmd)
}

// writeErrors 把 writeErrors 转成 error。
func writeErrors(resp Document) error {
	errs, ok := resp["writeErrors"].([]interface{})
	if !ok || len(errs) == 0 {
		return nil
	}
	first, _ := errs[0].(map[string]interface{})
	msg, _ := first["errmsg"].(string)
	if msg == "" {
		msg = fmt.Sprintf("%v", first)
	}
	return fmt.Errorf("mdb: write error: %s", msg)
}

// parseUpdateResult 解析 update 的应答。
func parseUpdateResult(resp Document) *UpdateResult {
	result := &UpdateResult{}
	if n, ok := resp["n"].(float64); ok {
		result.MatchedCount = int64(n)
	}
	if n, ok := resp["nModified"].(float64); ok {
		result.ModifiedCount = int64(n)
	}
	if upserted, ok := resp["upserted"].([]interface{}); ok && len(upserted) > 0 {
		if entry, ok := upserted[0].(map[string]interface{}); ok {
			result.UpsertedID = entry["_id"]
		}
	}
	return result
}
