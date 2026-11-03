package mdb

import (
	"context"
	"fmt"
)

// ChangeStream 是 change stream 句柄。
//
// 服务端用 `aggregate` + `$changeStream` 打开流（官方驱动也是这个形态），
// 这里按单批游标实现：Next/Decode 取事件，ResumeToken 取最后一条的 `_id`。
type ChangeStream struct {
	client   *Client
	ns       string
	pipeline interface{}
	cur      *Cursor
	err      error
	token    interface{}
	current  Document
}

// watch 在指定集合上打开 change stream。
func (c *Client) watch(ctx context.Context, collName string, pipeline interface{}) (*ChangeStream, error) {
	db := c.opts.Database
	if db == "" {
		db = "test"
	}
	cs := &ChangeStream{
		client:   c,
		ns:       db + "." + collName,
		pipeline: pipeline,
	}
	return cs, cs.open(ctx)
}

func (cs *ChangeStream) open(ctx context.Context) error {
	stages := []interface{}{
		map[string]interface{}{
			"$changeStream": map[string]interface{}{},
		},
	}
	if cs.pipeline != nil {
		p, err := toPipeline(cs.pipeline)
		if err != nil {
			return err
		}
		for _, stage := range p {
			stages = append(stages, stage)
		}
	}

	dbName, coll := splitNamespace(cs.ns)
	resp, err := cs.client.native.Database(dbName).Command(map[string]interface{}{
		"aggregate": coll,
		"pipeline":  stages,
		"cursor":    map[string]interface{}{},
	})
	if err != nil {
		cs.err = err
		return fmt.Errorf("mdb: watch: %w", err)
	}
	if err := commandErr(resp); err != nil {
		cs.err = err
		return fmt.Errorf("mdb: watch: %w", err)
	}
	cs.cur = staticCursor(docsFromCursorReply(resp))
	return nil
}

// Next 是否还有事件。
func (cs *ChangeStream) Next(ctx context.Context) bool {
	if cs.cur == nil {
		if err := cs.open(ctx); err != nil {
			cs.err = err
			return false
		}
	}
	if !cs.cur.Next(ctx) {
		return false
	}
	var doc Document
	if err := cs.cur.Decode(&doc); err != nil {
		cs.err = err
		return false
	}
	cs.token = doc["_id"]
	cs.current = doc
	return true
}

// Decode 把当前事件写入 val（*Document）。
func (cs *ChangeStream) Decode(val interface{}) error {
	target, ok := val.(*Document)
	if !ok {
		return fmt.Errorf("mdb: decode target must be *mdb.Document, got %T", val)
	}
	if cs.current == nil {
		return fmt.Errorf("mdb: no change event to decode")
	}
	*target = cs.current
	return nil
}

// TryNext 非阻塞地取下一个事件。
func (cs *ChangeStream) TryNext(ctx context.Context) bool {
	return cs.Next(ctx)
}

// ResumeToken 返回最近一个 resume token。
func (cs *ChangeStream) ResumeToken() interface{} { return cs.token }

// ID 返回流的标识（这里用命名空间）。
func (cs *ChangeStream) ID() interface{} { return cs.ns }

// Close 关闭流。
func (cs *ChangeStream) Close(ctx context.Context) error {
	if cs.cur != nil {
		return cs.cur.Close(ctx)
	}
	return nil
}

// Err 返回流上的错误。
func (cs *ChangeStream) Err() error {
	if cs.err != nil {
		return cs.err
	}
	if cs.cur != nil {
		return cs.cur.Err()
	}
	return nil
}

// ResumeAfter 指定从哪个 token 之后恢复（重新打开流时生效）。
func (cs *ChangeStream) ResumeAfter(token interface{}) *ChangeStream {
	cs.token = token
	return cs
}

// StartAfter 指定从哪个 token 开始（重新打开流时生效）。
func (cs *ChangeStream) StartAfter(token interface{}) *ChangeStream {
	cs.token = token
	return cs
}

// StartAtOperationTime 预留：单批实现下直接返回自身。
func (cs *ChangeStream) StartAtOperationTime(t interface{}) *ChangeStream { return cs }

func splitNamespace(ns string) (db string, coll string) {
	for i := 0; i < len(ns); i++ {
		if ns[i] == '.' {
			return ns[:i], ns[i+1:]
		}
	}
	return ns, ""
}
