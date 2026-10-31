package mdb

import (
	"fmt"
	"reflect"
)

// MdbCursor iterates over query results.
type MdbCursor struct {
	conn      *MdbConnection
	dbName    string
	collName  string
	filter    map[string]interface{}
	sort      map[string]interface{}
	proj      map[string]interface{}
	limit     int32
	skip      int32
	batchSize int32

	batch    []map[string]interface{}
	pos      int
	nextBatch string
	exhausted bool
	fetched   bool
	err       error
}

// Sort sets the sort order.
func (c *MdbCursor) Sort(sort map[string]interface{}) *MdbCursor {
	c.sort = sort
	return c
}

// Limit sets the maximum number of documents to return.
func (c *MdbCursor) Limit(n int) *MdbCursor {
	c.limit = int32(n)
	return c
}

// Skip sets the number of documents to skip.
func (c *MdbCursor) Skip(n int) *MdbCursor {
	c.skip = int32(n)
	return c
}

// Projection sets the projection.
func (c *MdbCursor) Projection(proj map[string]interface{}) *MdbCursor {
	c.proj = proj
	return c
}

// BatchSize sets the batch size.
func (c *MdbCursor) BatchSize(n int) *MdbCursor {
	c.batchSize = int32(n)
	return c
}

// fetchBatch retrieves the next batch from the server.
func (c *MdbCursor) fetchBatch() error {
	cmd := map[string]interface{}{
		"find":  c.collName,
		"filter": c.filter,
		"batchSize": c.batchSize,
	}
	if c.sort != nil {
		cmd["sort"] = c.sort
	}
	if c.proj != nil {
		cmd["projection"] = c.proj
	}
	if c.limit > 0 {
		cmd["limit"] = c.limit
	}
	if c.skip > 0 {
		cmd["skip"] = c.skip
	}

	resp, err := c.conn.SendCommand(cmd)
	if err != nil {
		return err
	}

	cursorResp, ok := resp["cursor"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("mdb: no cursor in response")
	}

	batch, ok := cursorResp["firstBatch"].([]interface{})
	if !ok {
		c.exhausted = true
		return nil
	}

	c.batch = make([]map[string]interface{}, 0, len(batch))
	for _, item := range batch {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		c.batch = append(c.batch, m)
	}

	id, _ := cursorResp["id"].(float64)
	if id == 0 {
		c.exhausted = true
	}
	nextBatch, _ := cursorResp["id"].(float64)
	c.nextBatch = fmt.Sprintf("%v", nextBatch)
	c.pos = 0
	c.fetched = true

	// For limit: trim batch
	if c.limit > 0 && len(c.batch) > int(c.limit) {
		c.batch = c.batch[:c.limit]
	}

	return nil
}

// Next advances the cursor to the next document.
func (c *MdbCursor) Next() bool {
	if !c.fetched {
		if err := c.fetchBatch(); err != nil {
			c.err = err
			return false
		}
	}
	if c.pos < len(c.batch) {
		c.pos++
		return true
	}
	if c.exhausted {
		return false
	}
	// Fetch next batch using getMore
	if err := c.fetchMore(); err != nil {
		c.err = err
		return false
	}
	if c.pos < len(c.batch) {
		c.pos++
		return true
	}
	return false
}

func (c *MdbCursor) fetchMore() error {
	cmd := map[string]interface{}{
		"getMore": c.nextBatch,
		"batchSize": c.batchSize,
	}
	resp, err := c.conn.SendCommand(cmd)
	if err != nil {
		return err
	}

	cursorResp, ok := resp["cursor"].(map[string]interface{})
	if !ok {
		c.exhausted = true
		return nil
	}

	batch, ok := cursorResp["batch"].([]interface{})
	if !ok || len(batch) == 0 {
		c.exhausted = true
		return nil
	}

	c.batch = make([]map[string]interface{}, 0, len(batch))
	for _, item := range batch {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		c.batch = append(c.batch, m)
	}
	c.pos = 0

	id, _ := cursorResp["id"].(float64)
	if id == 0 {
		c.exhausted = true
	}
	c.nextBatch = fmt.Sprintf("%v", id)

	return nil
}

// Decode decodes the current document into v.
func (c *MdbCursor) Decode(v interface{}) error {
	if c.pos == 0 || c.pos > len(c.batch) {
		return fmt.Errorf("mdb: no document to decode")
	}
	doc := c.batch[c.pos-1]
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Map {
		return fmt.Errorf("mdb: decode target must be a pointer to map")
	}
	rv.Elem().Set(reflect.ValueOf(doc))
	return nil
}

// All returns all documents in the cursor.
func (c *MdbCursor) All() ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	for c.Next() {
		var doc map[string]interface{}
		if err := c.Decode(&doc); err != nil {
			return nil, err
		}
		result = append(result, doc)
	}
	if c.err != nil {
		return nil, c.err
	}
	return result, nil
}

// Close closes the cursor.
func (c *MdbCursor) Close() error {
	return nil
}
