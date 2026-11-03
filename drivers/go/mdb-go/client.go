package mdb

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	native "github.com/mdb-official/mdb-native"
)

// Client 是 MDB 的客户端入口。
//
// 与旧实现（包装 mongo-go-driver）不同，这里完全建立在依赖自足的
// `mdb-native` 之上：不需要 go.sum、不需要联网拉第三方模块。
type Client struct {
	native *native.MdbClient
	opts   *ClientOptions
	host   string
	port   int
}

// NewClient 连接指定的 host:port。
func NewClient(host string, port int, opts ...ClientOption) (*Client, error) {
	o := applyOptions(opts)
	c := native.NewClient(host, port)
	if err := c.Connect(); err != nil {
		return nil, fmt.Errorf("mdb: failed to connect: %w", err)
	}
	return &Client{native: c, opts: o, host: host, port: port}, nil
}

// NewClientFromURI 用连接串创建客户端（`mongodb://[user:pass@]host[:port][/db]`）。
func NewClientFromURI(uri string, opts ...ClientOption) (*Client, error) {
	host, port, username, password, authDB, db, err := parseURI(uri)
	if err != nil {
		return nil, err
	}
	c, err := NewClient(host, port, opts...)
	if err != nil {
		return nil, err
	}
	if username != "" {
		if err := c.Authenticate(username, password, authDB); err != nil {
			_ = c.Close(context.Background())
			return nil, err
		}
	}
	if db != "" {
		c.opts.Database = db
	}
	return c, nil
}

// NewClientWithAuth 用账号口令连接。
func NewClientWithAuth(host string, port int, username, password, authDB string) (*Client, error) {
	c, err := NewClient(host, port)
	if err != nil {
		return nil, err
	}
	if err := c.Authenticate(username, password, authDB); err != nil {
		_ = c.Close(context.Background())
		return nil, err
	}
	return c, nil
}

// parseURI 解析最小可用的连接串（够用即可，不追求完整 SRV/副本集语法）。
func parseURI(raw string) (host string, port int, username, password, authDB, db string, err error) {
	port = 27017
	host = "localhost"
	if !strings.HasPrefix(raw, "mongodb://") && !strings.HasPrefix(raw, "mongodb+srv://") {
		raw = "mongodb://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", 0, "", "", "", "", fmt.Errorf("mdb: invalid uri: %w", err)
	}
	if u.Hostname() != "" {
		host = u.Hostname()
	}
	if u.Port() != "" {
		if _, err := fmt.Sscanf(u.Port(), "%d", &port); err != nil {
			return "", 0, "", "", "", "", fmt.Errorf("mdb: invalid port in uri: %w", err)
		}
	}
	if u.User != nil {
		username = u.User.Username()
		password, _ = u.User.Password()
	}
	db = strings.TrimPrefix(u.Path, "/")
	// authSource 优先，其次用连接串里的库名，最后退回 admin。
	authDB = u.Query().Get("authSource")
	if authDB == "" {
		authDB = db
	}
	if authDB == "" {
		authDB = "admin"
	}
	return host, port, username, password, authDB, db, nil
}

// Database 返回数据库句柄。
func (c *Client) Database(name string) *Database {
	return &Database{db: c.native.Database(name), client: c}
}

// ListDatabases 返回所有数据库名。
func (c *Client) ListDatabases(ctx context.Context) ([]string, error) {
	return c.native.ListDatabases()
}

// ServerStatus 返回 serverStatus 的结果。
func (c *Client) ServerStatus(ctx context.Context) (Document, error) {
	return c.native.ServerStatus()
}

// ReplicaSetStatus 返回 replSetGetStatus 的结果。
func (c *Client) ReplicaSetStatus(ctx context.Context) (Document, error) {
	db := c.native.Database("admin")
	return db.Command(Document{"replSetGetStatus": 1})
}

// Ping 检查连接是否存活。
func (c *Client) Ping(ctx context.Context) error {
	if !c.native.Ping() {
		return errors.New("mdb: ping failed")
	}
	return nil
}

// IsConnected 连接是否可用。
func (c *Client) IsConnected() bool {
	return c.native != nil && c.native.Ping()
}

// Close 关闭连接。
func (c *Client) Close(ctx context.Context) error {
	if c.native == nil {
		return nil
	}
	return c.native.Close()
}

// StartSession 创建一个会话（事务需要它来携带 lsid）。
func (c *Client) StartSession(opts ...SessionOptions) (*Session, error) {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return nil, fmt.Errorf("mdb: generate session id: %w", err)
	}
	return &Session{client: c, id: id}, nil
}

// Authenticate 用 SCRAM-SHA-256 完成认证。
func (c *Client) Authenticate(username, password, authDB string) error {
	if authDB == "" {
		authDB = "admin"
	}
	return scramSHA256(c.native.Database(authDB), username, password)
}

// ObjectID 生成一个 12 字节的 ObjectId（24 位十六进制字符串）。
func ObjectID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		// 随机源不可用时退回时间戳，绝不返回空 id。
		now := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(now >> (8 * uint(i)))
		}
	}
	return hex.EncodeToString(b)
}
