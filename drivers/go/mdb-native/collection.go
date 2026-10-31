package mdb

import (
	"fmt"
)

// MdbCollection provides access to a specific collection.
type MdbCollection struct {
	db   *MdbDatabase
	name string
}

// Name returns the collection name.
func (c *MdbCollection) Name() string {
	return c.name
}

// InsertOneResult is returned by InsertOne.
type InsertOneResult struct {
	InsertedID interface{}
	InsertedCount int64
}

// InsertManyResult is returned by InsertMany.
type InsertManyResult struct {
	InsertedIDs   []interface{}
	InsertedCount int64
}

// UpdateResult is returned by UpdateOne/UpdateMany.
type UpdateResult struct {
	MatchedCount  int64
	ModifiedCount int64
	UpsertedCount int64
	UpsertedID    interface{}
}

// DeleteResult is returned by DeleteOne/DeleteMany.
type DeleteResult struct {
	DeletedCount int64
}

// InsertOne inserts a single document.
func (c *MdbCollection) InsertOne(doc map[string]interface{}) (*InsertOneResult, error) {
	resp, err := c.db.client.conn.SendCommandWithDocs(
		map[string]interface{}{
			"insert": c.name,
			"ordered": true,
		},
		[]map[string]interface{}{doc},
		"documents",
	)
	if err != nil {
		return nil, err
	}
	if writeErrors, ok := resp["writeErrors"].([]interface{}); ok && len(writeErrors) > 0 {
		return nil, fmt.Errorf("insert failed: %v", writeErrors[0])
	}
	n, _ := resp["n"].(float64)
	return &InsertOneResult{
		InsertedCount: int64(n),
	}, nil
}

// InsertMany inserts multiple documents.
func (c *MdbCollection) InsertMany(docs []map[string]interface{}) (*InsertManyResult, error) {
	resp, err := c.db.client.conn.SendCommandWithDocs(
		map[string]interface{}{
			"insert":  c.name,
			"ordered": true,
		},
		docs,
		"documents",
	)
	if err != nil {
		return nil, err
	}
	if writeErrors, ok := resp["writeErrors"].([]interface{}); ok && len(writeErrors) > 0 {
		return nil, fmt.Errorf("insert failed: %v", writeErrors[0])
	}
	n, _ := resp["n"].(float64)
	return &InsertManyResult{
		InsertedCount: int64(n),
	}, nil
}

// Find returns a cursor for the given filter.
func (c *MdbCollection) Find(filter map[string]interface{}) *MdbCursor {
	cursor := &MdbCursor{
		conn:     c.db.client.conn,
		dbName:   c.db.name,
		collName: c.name,
		filter:   filter,
		batchSize: 101,
	}
	return cursor
}

// FindOne returns a single document matching the filter.
func (c *MdbCollection) FindOne(filter map[string]interface{}) (map[string]interface{}, error) {
	cursor := c.Find(filter).Limit(1)
	if !cursor.Next() {
		return nil, nil
	}
	var doc map[string]interface{}
	if err := cursor.Decode(&doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// UpdateOne updates a single document matching the filter.
func (c *MdbCollection) UpdateOne(filter, update map[string]interface{}) (*UpdateResult, error) {
	resp, err := c.db.client.conn.SendCommand(map[string]interface{}{
		"update": c.name,
		"updates": []interface{}{
			map[string]interface{}{
				"q":      filter,
				"u":      update,
				"upsert": false,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return parseUpdateResult(resp)
}

// UpdateMany updates multiple documents matching the filter.
func (c *MdbCollection) UpdateMany(filter, update map[string]interface{}) (*UpdateResult, error) {
	resp, err := c.db.client.conn.SendCommand(map[string]interface{}{
		"update": c.name,
		"updates": []interface{}{
			map[string]interface{}{
				"q":      filter,
				"u":      update,
				"multi":  true,
				"upsert": false,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return parseUpdateResult(resp)
}

func parseUpdateResult(resp map[string]interface{}) (*UpdateResult, error) {
	if writeErrors, ok := resp["writeErrors"].([]interface{}); ok && len(writeErrors) > 0 {
		return nil, fmt.Errorf("update failed: %v", writeErrors[0])
	}
	n, _ := resp["n"].(float64)
	nModified, _ := resp["nModified"].(float64)
	return &UpdateResult{
		MatchedCount:  int64(n),
		ModifiedCount: int64(nModified),
	}, nil
}

// DeleteOne deletes a single document matching the filter.
func (c *MdbCollection) DeleteOne(filter map[string]interface{}) (*DeleteResult, error) {
	resp, err := c.db.client.conn.SendCommand(map[string]interface{}{
		"delete": c.name,
		"deletes": []interface{}{
			map[string]interface{}{
				"q":   filter,
				"limit": 1,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if writeErrors, ok := resp["writeErrors"].([]interface{}); ok && len(writeErrors) > 0 {
		return nil, fmt.Errorf("delete failed: %v", writeErrors[0])
	}
	n, _ := resp["n"].(float64)
	return &DeleteResult{DeletedCount: int64(n)}, nil
}

// DeleteMany deletes multiple documents matching the filter.
func (c *MdbCollection) DeleteMany(filter map[string]interface{}) (*DeleteResult, error) {
	resp, err := c.db.client.conn.SendCommand(map[string]interface{}{
		"delete": c.name,
		"deletes": []interface{}{
			map[string]interface{}{
				"q":   filter,
				"limit": 0,
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if writeErrors, ok := resp["writeErrors"].([]interface{}); ok && len(writeErrors) > 0 {
		return nil, fmt.Errorf("delete failed: %v", writeErrors[0])
	}
	n, _ := resp["n"].(float64)
	return &DeleteResult{DeletedCount: int64(n)}, nil
}

// Aggregate runs an aggregation pipeline.
func (c *MdbCollection) Aggregate(pipeline []map[string]interface{}) (map[string]interface{}, error) {
	return c.db.client.conn.SendCommand(map[string]interface{}{
		"aggregate": c.name,
		"pipeline":  pipeline,
		"cursor":    map[string]interface{}{},
	})
}

// Distinct returns distinct values for a key.
func (c *MdbCollection) Distinct(key string, filter map[string]interface{}) (interface{}, error) {
	resp, err := c.db.client.conn.SendCommand(map[string]interface{}{
		"distinct": c.name,
		"key":      key,
		"query":    filter,
	})
	if err != nil {
		return nil, err
	}
	return resp["values"], nil
}

// CountDocuments returns the count of documents matching the filter.
func (c *MdbCollection) CountDocuments(filter map[string]interface{}) (int64, error) {
	resp, err := c.db.client.conn.SendCommand(map[string]interface{}{
		"count": c.name,
		"query": filter,
	})
	if err != nil {
		return 0, err
	}
	n, ok := resp["n"].(float64)
	if !ok {
		return 0, nil
	}
	return int64(n), nil
}

// CreateIndex creates an index on the collection.
func (c *MdbCollection) CreateIndex(keys map[string]interface{}) error {
	_, err := c.db.client.conn.SendCommand(map[string]interface{}{
		"createIndexes": c.name,
		"indexes": []interface{}{
			map[string]interface{}{
				"key": keys,
			},
		},
	})
	return err
}

// DropIndex drops an index by name.
func (c *MdbCollection) DropIndex(name string) error {
	_, err := c.db.client.conn.SendCommand(map[string]interface{}{
		"dropIndexes": c.name,
		"index":       name,
	})
	return err
}

// ListIndexes returns all indexes on the collection.
func (c *MdbCollection) ListIndexes() ([]map[string]interface{}, error) {
	resp, err := c.db.client.conn.SendCommand(map[string]interface{}{
		"listIndexes": c.name,
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
	var result []map[string]interface{}
	for _, item := range batch {
		if m, ok := item.(map[string]interface{}); ok {
			result = append(result, m)
		}
	}
	return result, nil
}

// Stats returns collection statistics.
func (c *MdbCollection) Stats() (map[string]interface{}, error) {
	return c.db.client.conn.SendCommand(map[string]interface{}{
		"collStats": c.name,
	})
}

// Explain returns the query explanation (explain plan).
func (c *MdbCollection) Explain(filter map[string]interface{}) (map[string]interface{}, error) {
	return c.db.client.conn.SendCommand(map[string]interface{}{
		"explain": map[string]interface{}{
			"find":  c.name,
			"filter": filter,
		},
	})
}
