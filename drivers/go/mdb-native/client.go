package mdb

// MdbClient provides a high-level API for MDB operations.
type MdbClient struct {
	conn *MdbConnection
	host string
	port int
}

// NewClient creates a new MdbClient. Call Connect() to establish the connection.
func NewClient(host string, port int) *MdbClient {
	return &MdbClient{host: host, port: port}
}

// Connect establishes the TCP connection to the server.
func (c *MdbClient) Connect() error {
	conn, err := Dial(c.host, c.port)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// Close closes the connection.
func (c *MdbClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Database returns a database handle.
func (c *MdbClient) Database(name string) *MdbDatabase {
	return &MdbDatabase{client: c, name: name}
}

// ListDatabases returns the names of all databases.
func (c *MdbClient) ListDatabases() ([]string, error) {
	resp, err := c.conn.SendCommand(map[string]interface{}{
		"listDatabases": 1,
	})
	if err != nil {
		return nil, err
	}
	databases, ok := resp["databases"].([]interface{})
	if !ok {
		return nil, nil
	}
	var names []string
	for _, db := range databases {
		m, ok := db.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		names = append(names, name)
	}
	return names, nil
}

// Ping verifies the server is alive.
func (c *MdbClient) Ping() bool {
	resp, err := c.conn.SendCommand(map[string]interface{}{
		"ping": 1,
	})
	if err != nil {
		return false
	}
	ok, _ := resp["ok"].(float64)
	return ok == 1
}

// ServerStatus returns server status information.
func (c *MdbClient) ServerStatus() (map[string]interface{}, error) {
	return c.conn.SendCommand(map[string]interface{}{
		"serverStatus": 1,
	})
}
