package mdb

// MdbDatabase provides access to a specific database.
type MdbDatabase struct {
	client *MdbClient
	name   string
}

// Name returns the database name.
func (d *MdbDatabase) Name() string {
	return d.name
}

// Collection returns a collection handle.
func (d *MdbDatabase) Collection(name string) *MdbCollection {
	return &MdbCollection{db: d, name: name}
}

// ListCollections returns the names of all collections.
func (d *MdbDatabase) ListCollections() ([]string, error) {
	resp, err := d.client.conn.SendCommand(map[string]interface{}{
		"listCollections": 1,
	})
	if err != nil {
		return nil, err
	}
	cursor, ok := resp["cursor"].(map[string]interface{})
	if !ok {
		return nil, nil
	}
	batch, ok := cursor["firstBatch"].([]interface{})
	if !ok {
		return nil, nil
	}
	var names []string
	for _, item := range batch {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := m["name"].(string)
		names = append(names, name)
	}
	return names, nil
}

// CreateCollection creates a new collection.
func (d *MdbDatabase) CreateCollection(name string) error {
	resp, err := d.client.conn.SendCommand(map[string]interface{}{
		"create": name,
	})
	if err != nil {
		return err
	}
	ok, _ := resp["ok"].(float64)
	if ok != 1 {
		return errFromResponse(resp)
	}
	return nil
}

// DropCollection drops a collection.
func (d *MdbDatabase) DropCollection(name string) error {
	resp, err := d.client.conn.SendCommand(map[string]interface{}{
		"drop": name,
	})
	if err != nil {
		return err
	}
	ok, _ := resp["ok"].(float64)
	if ok != 1 {
		return errFromResponse(resp)
	}
	return nil
}

// Command runs an arbitrary command on the database.
func (d *MdbDatabase) Command(cmd map[string]interface{}) (map[string]interface{}, error) {
	return d.client.conn.SendCommand(cmd)
}

// Stats returns database statistics.
func (d *MdbDatabase) Stats() (map[string]interface{}, error) {
	return d.client.conn.SendCommand(map[string]interface{}{
		"dbStats": 1,
	})
}

func errFromResponse(resp map[string]interface{}) error {
	if errmsg, ok := resp["errmsg"].(string); ok {
		return &mdbError{msg: errmsg}
	}
	return nil
}

type mdbError struct {
	msg string
}

func (e *mdbError) Error() string {
	return e.msg
}
