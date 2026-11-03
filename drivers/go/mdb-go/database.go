package mdb

import (
	"context"
	"fmt"

	native "github.com/mdb-official/mdb-native"
)

// Database 是数据库句柄。
type Database struct {
	db     *native.MdbDatabase
	client *Client
	txn    *Transaction
}

// Collection 返回集合句柄（继承所在事务上下文）。
func (d *Database) Collection(name string) *Collection {
	return &Collection{coll: d.db.Collection(name), db: d, txn: d.txn}
}

// ListCollections 返回库内所有集合名。
func (d *Database) ListCollections(ctx context.Context) ([]string, error) {
	return d.db.ListCollections()
}

// CreateCollection 创建集合。
func (d *Database) CreateCollection(ctx context.Context, name string, opts ...*CreateCollectionOptions) error {
	cmd := map[string]interface{}{"create": name}
	if len(opts) > 0 && opts[0] != nil {
		o := opts[0]
		if o.Capped {
			cmd["capped"] = true
			if o.SizeInBytes > 0 {
				cmd["size"] = o.SizeInBytes
			}
			if o.MaxDocuments > 0 {
				cmd["max"] = o.MaxDocuments
			}
		}
		if o.ValidationLevel != "" {
			cmd["validationLevel"] = o.ValidationLevel
		}
	}
	resp, err := d.db.Command(cmd)
	if err != nil {
		return fmt.Errorf("mdb: create collection %q: %w", name, err)
	}
	return commandErr(resp)
}

// DropCollection 删除集合。
func (d *Database) DropCollection(ctx context.Context, name string) error {
	if err := d.db.DropCollection(name); err != nil {
		return fmt.Errorf("mdb: drop collection %q: %w", name, err)
	}
	return nil
}

// RunCommand 执行任意数据库命令。
func (d *Database) RunCommand(ctx context.Context, cmd interface{}) (Document, error) {
	doc, err := toDocument(cmd)
	if err != nil {
		return nil, err
	}
	resp, err := d.db.Command(doc)
	if err != nil {
		return nil, fmt.Errorf("mdb: run command: %w", err)
	}
	if err := commandErr(resp); err != nil {
		return resp, err
	}
	return resp, nil
}

// Stats 返回 dbStats 结果。
func (d *Database) Stats(ctx context.Context) (Document, error) {
	return d.db.Stats()
}

// Role 是用户角色。
type Role struct {
	Role string
	DB   string
}

// CreateUser 创建用户。
func (d *Database) CreateUser(ctx context.Context, username, password string, roles []Role) error {
	bsonRoles := make([]interface{}, len(roles))
	for i, r := range roles {
		bsonRoles[i] = map[string]interface{}{"role": r.Role, "db": r.DB}
	}
	resp, err := d.db.Command(map[string]interface{}{
		"createUser": username,
		"pwd":        password,
		"roles":      bsonRoles,
	})
	if err != nil {
		return fmt.Errorf("mdb: create user %q: %w", username, err)
	}
	return commandErr(resp)
}

// DropUser 删除用户。
func (d *Database) DropUser(ctx context.Context, username string) error {
	resp, err := d.db.Command(map[string]interface{}{"dropUser": username})
	if err != nil {
		return fmt.Errorf("mdb: drop user %q: %w", username, err)
	}
	return commandErr(resp)
}
