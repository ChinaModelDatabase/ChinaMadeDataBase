package mdb

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Session 是客户端会话：负责生成 lsid 与事务序号。
type Session struct {
	client *Client
	id     []byte
	txnSeq int64
}

// ID 返回会话的 lsid 文档（wire 层要求 `lsid: {id: Binary}`）。
func (s *Session) ID() Document {
	return Document{
		"id": binarySubtype4(s.id),
	}
}

// EndSession 结束会话（当前实现为无操作占位）。
func (s *Session) EndSession(ctx context.Context) {}

// Transaction 是一次多文档事务。
//
// 服务端按 `lsid` 记录事务：事务里的写命令要带 `startTransaction`
// （仅第一条）、`txnNumber` 与 `autocommit: false`，提交/回滚再发
// 对应的命令。少了任何一项，commit 都会报"没有活跃事务"。
type Transaction struct {
	client     *Client
	session    *Session
	txnNumber  int64
	started    bool
	txnCounter int64
}

// StartTransaction 开启事务。
func (c *Client) StartTransaction(opts ...TransactionOptions) (*Transaction, error) {
	session, err := c.StartSession()
	if err != nil {
		return nil, err
	}
	return &Transaction{
		client:    c,
		session:   session,
		txnNumber: atomic.AddInt64(&session.txnSeq, 1),
	}, nil
}

// Database 返回事务内的数据库句柄。
func (t *Transaction) Database(name string) *Database {
	return &Database{db: t.client.native.Database(name), client: t.client, txn: t}
}

// Collection 返回事务内的集合句柄：它的写操作会被缓冲，直到 Commit。
func (t *Transaction) Collection(name string) *Collection {
	return t.Database(t.client.opts.Database).Collection(name)
}

// Commit 提交事务。
func (t *Transaction) Commit() error {
	resp, err := t.command("commitTransaction")
	if err != nil {
		return err
	}
	t.started = false
	return commandErr(resp)
}

// Abort 回滚事务（丢弃缓冲的写操作）。
func (t *Transaction) Abort() error {
	resp, err := t.command("abortTransaction")
	if err != nil {
		return err
	}
	t.started = false
	return commandErr(resp)
}

func (t *Transaction) command(name string) (Document, error) {
	db := t.client.opts.Database
	if db == "" {
		db = "admin"
	}
	resp, err := t.client.native.Database(db).Command(Document{
		name:         1,
		"lsid":       t.session.ID(),
		"txnNumber":  t.txnNumber,
		"autocommit": false,
	})
	if err != nil {
		return nil, fmt.Errorf("mdb: %s: %w", name, err)
	}
	return resp, nil
}

// apply 把会话字段写进命令：事务里的第一条写命令额外带 startTransaction。
func (t *Transaction) apply(cmd map[string]interface{}) {
	cmd["lsid"] = t.session.ID()
	cmd["txnNumber"] = t.txnNumber
	cmd["autocommit"] = false
	if !t.started {
		cmd["startTransaction"] = true
		t.started = true
	}
}

// WithTransaction 起事务、执行 fn，出错自动回滚，否则提交。
func (c *Client) WithTransaction(ctx context.Context, fn func(*Transaction) (interface{}, error)) (interface{}, error) {
	txn, err := c.StartTransaction()
	if err != nil {
		return nil, err
	}
	result, err := fn(txn)
	if err != nil {
		if abortErr := txn.Abort(); abortErr != nil {
			return nil, fmt.Errorf("mdb: transaction failed (%v) and abort failed: %w", err, abortErr)
		}
		return nil, err
	}
	if err := txn.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}
