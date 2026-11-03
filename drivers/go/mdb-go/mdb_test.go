package mdb

import (
	"context"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	client, err := NewClient("localhost", 27017)
	if err != nil {
		t.Skipf("Skipping: cannot connect to MDB: %v", err)
	}
	defer client.Close(context.Background())

	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewClientFromURI(t *testing.T) {
	client, err := NewClientFromURI("mongodb://localhost:27017")
	if err != nil {
		t.Skipf("Skipping: cannot connect to MDB: %v", err)
	}
	defer client.Close(context.Background())

	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewClientWithAuth(t *testing.T) {
	client, err := NewClientWithAuth("localhost", 27017, "admin", "password", "admin")
	if err != nil {
		t.Skipf("Skipping: cannot connect to MDB with auth: %v", err)
	}
	defer client.Close(context.Background())

	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestClientOptions(t *testing.T) {
	client, err := NewClient("localhost", 27017,
		WithAppName("test-app"),
		WithMaxPoolSize(100),
		WithMinPoolSize(10),
		WithTimeout(5*time.Second),
		WithWriteConcern(WriteConcernMajority),
		WithReadConcern(ReadConcernMajority),
	)
	if err != nil {
		t.Skipf("Skipping: cannot connect to MDB: %v", err)
	}
	defer client.Close(context.Background())

	if client.opts.AppName != "test-app" {
		t.Errorf("expected app name 'test-app', got %q", client.opts.AppName)
	}
	if client.opts.MaxPoolSize != 100 {
		t.Errorf("expected max pool size 100, got %d", client.opts.MaxPoolSize)
	}
	if client.opts.MinPoolSize != 10 {
		t.Errorf("expected min pool size 10, got %d", client.opts.MinPoolSize)
	}
}

func TestWriteConcern(t *testing.T) {
	if WriteConcernAcknowledged.Level != "acknowledged" {
		t.Errorf("expected 'acknowledged', got %q", WriteConcernAcknowledged.Level)
	}
	if WriteConcernMajority.Level != "majority" {
		t.Errorf("expected 'majority', got %q", WriteConcernMajority.Level)
	}
}

func TestReadConcern(t *testing.T) {
	tests := []struct {
		concern ReadConcern
		level   string
	}{
		{ReadConcernLocal, "local"},
		{ReadConcernMajority, "majority"},
		{ReadConcernLinearizable, "linearizable"},
		{ReadConcernSnapshot, "snapshot"},
	}
	for _, tt := range tests {
		if tt.concern.Level != tt.level {
			t.Errorf("expected %q, got %q", tt.level, tt.concern.Level)
		}
	}
}

func TestIndexKeys(t *testing.T) {
	asc := Ascending("a", "b")
	if len(asc) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(asc))
	}
	if asc[0].Value != 1 || asc[1].Value != 1 {
		t.Errorf("expected ascending (1), got %v", asc)
	}

	desc := Descending("x", "y")
	if len(desc) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(desc))
	}
	if desc[0].Value != -1 || desc[1].Value != -1 {
		t.Errorf("expected descending (-1), got %v", desc)
	}

	text := TextIndex("title", "body")
	if len(text) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(text))
	}
	if text[0].Value != "text" {
		t.Errorf("expected 'text', got %v", text[0].Value)
	}

	geo := GeoSphereIndex("location")
	if len(geo) != 1 {
		t.Fatalf("expected 1 element, got %d", len(geo))
	}
	if geo[0].Value != "2dsphere" {
		t.Errorf("expected '2dsphere', got %v", geo[0].Value)
	}

	hashed := HashedIndex("email")
	if len(hashed) != 1 {
		t.Fatalf("expected 1 element, got %d", len(hashed))
	}
	if hashed[0].Value != "hashed" {
		t.Errorf("expected 'hashed', got %v", hashed[0].Value)
	}
}

func TestIntegrationOperations(t *testing.T) {
	client, err := NewClient("localhost", 27017)
	if err != nil {
		t.Skipf("Skipping integration tests: cannot connect to MDB: %v", err)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := client.Database("mdb_test_integration")
	defer db.DropCollection(ctx, "test_collection")

	coll := db.Collection("test_collection")

	// Insert one
	insertResult, err := coll.InsertOne(ctx, Document{
		"name":  "test",
		"count": 1,
		"time":  time.Now(),
	})
	if err != nil {
		t.Fatalf("InsertOne failed: %v", err)
	}
	if insertResult.InsertedID == nil {
		t.Fatal("expected non-nil InsertedID")
	}

	// Insert many
	docs := []interface{}{
		Document{"name": "doc1", "count": 2},
		Document{"name": "doc2", "count": 3},
		Document{"name": "doc3", "count": 4},
	}
	insertManyResult, err := coll.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("InsertMany failed: %v", err)
	}
	if len(insertManyResult.InsertedIDs) != 3 {
		t.Errorf("expected 3 inserted IDs, got %d", len(insertManyResult.InsertedIDs))
	}

	// Count documents
	count, err := coll.CountDocuments(ctx, Document{})
	if err != nil {
		t.Fatalf("CountDocuments failed: %v", err)
	}
	if count != 4 {
		t.Errorf("expected count 4, got %d", count)
	}

	// Find one
	result := Document{}
	sr, err := coll.FindOne(ctx, Document{"name": "test"})
	if err != nil {
		t.Fatalf("FindOne failed: %v", err)
	}
	err = sr.Decode(&result)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if result["name"] != "test" {
		t.Errorf("expected name 'test', got %v", result["name"])
	}

	// Find
	cursor, err := coll.Find(ctx, Document{"count": Document{"$gte": 3}})
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	var results []Document
	err = cursor.All(ctx, &results)
	if err != nil {
		t.Fatalf("cursor.All failed: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	// Update one
	updateResult, err := coll.UpdateOne(ctx, Document{"name": "test"}, Document{"$set": Document{"count": 10}})
	if err != nil {
		t.Fatalf("UpdateOne failed: %v", err)
	}
	if updateResult.MatchedCount != 1 {
		t.Errorf("expected matched 1, got %d", updateResult.MatchedCount)
	}

	// Update many
	updateManyResult, err := coll.UpdateMany(ctx, Document{"count": Document{"$gte": 3}}, Document{"$inc": Document{"count": 100}})
	if err != nil {
		t.Fatalf("UpdateMany failed: %v", err)
	}
	if updateManyResult.ModifiedCount != 2 {
		t.Errorf("expected modified 2, got %d", updateManyResult.ModifiedCount)
	}

	// Replace one
	replaceResult, err := coll.ReplaceOne(ctx, Document{"name": "doc1"}, Document{"name": "doc1_replaced", "count": 999})
	if err != nil {
		t.Fatalf("ReplaceOne failed: %v", err)
	}
	if replaceResult.MatchedCount != 1 {
		t.Errorf("expected matched 1, got %d", replaceResult.MatchedCount)
	}

	// Delete one
	deleteResult, err := coll.DeleteOne(ctx, Document{"name": "doc1_replaced"})
	if err != nil {
		t.Fatalf("DeleteOne failed: %v", err)
	}
	if deleteResult.DeletedCount != 1 {
		t.Errorf("expected deleted 1, got %d", deleteResult.DeletedCount)
	}

	// Distinct
	distinctResult, err := coll.Distinct(ctx, "name", Document{})
	if err != nil {
		t.Fatalf("Distinct failed: %v", err)
	}
	if len(distinctResult) != 3 {
		t.Errorf("expected 3 distinct values, got %d", len(distinctResult))
	}

	// Aggregate
	aggPipeline := []interface{}{
		Document{"$group": Document{"_id": "$count", "total": Document{"$sum": 1}}},
	}
	aggCursor, err := coll.Aggregate(ctx, aggPipeline)
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}
	var aggResults []Document
	err = aggCursor.All(ctx, &aggResults)
	if err != nil {
		t.Fatalf("Aggregate All failed: %v", err)
	}
	if len(aggResults) == 0 {
		t.Error("expected at least one aggregation result")
	}
}

func TestIntegrationIndex(t *testing.T) {
	client, err := NewClient("localhost", 27017)
	if err != nil {
		t.Skipf("Skipping index tests: cannot connect to MDB: %v", err)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := client.Database("mdb_test_index")
	defer db.DropCollection(ctx, "index_collection")

	coll := db.Collection("index_collection")

	// Create index
	indexName, err := coll.CreateIndex(ctx, Ascending("name"))
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}
	if indexName == "" {
		t.Error("expected non-empty index name")
	}

	// List indexes
	cursor, err := coll.ListIndexes(ctx)
	if err != nil {
		t.Fatalf("ListIndexes failed: %v", err)
	}
	var indexes []Document
	err = cursor.All(ctx, &indexes)
	if err != nil {
		t.Fatalf("ListIndexes All failed: %v", err)
	}
	if len(indexes) < 1 {
		t.Errorf("expected at least 1 index, got %d", len(indexes))
	}

	// Drop index
	err = coll.DropIndex(ctx, indexName)
	if err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}
}

func TestIntegrationDatabase(t *testing.T) {
	client, err := NewClient("localhost", 27017)
	if err != nil {
		t.Skipf("Skipping database tests: cannot connect to MDB: %v", err)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := client.Database("mdb_test_db_ops")

	// Create collection
	err = db.CreateCollection(ctx, "new_collection")
	if err != nil {
		t.Fatalf("CreateCollection failed: %v", err)
	}

	// List collections
	collections, err := db.ListCollections(ctx)
	if err != nil {
		t.Fatalf("ListCollections failed: %v", err)
	}
	found := false
	for _, name := range collections {
		if name == "new_collection" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected to find 'new_collection' in list")
	}

	// Drop collection
	err = db.DropCollection(ctx, "new_collection")
	if err != nil {
		t.Fatalf("DropCollection failed: %v", err)
	}

	// Stats
	stats, err := db.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats == nil {
		t.Error("expected non-nil stats")
	}
}

func TestIntegrationServerStatus(t *testing.T) {
	client, err := NewClient("localhost", 27017)
	if err != nil {
		t.Skipf("Skipping server status test: cannot connect to MDB: %v", err)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	status, err := client.ServerStatus(ctx)
	if err != nil {
		t.Fatalf("ServerStatus failed: %v", err)
	}
	if status == nil {
		t.Error("expected non-nil status")
	}

	dbNames, err := client.ListDatabases(ctx)
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	if dbNames == nil {
		t.Error("expected non-nil database list")
	}
}

func TestIntegrationObjectID(t *testing.T) {
	client, err := NewClient("localhost", 27017)
	if err != nil {
		t.Skipf("Skipping ObjectID tests: cannot connect to MDB: %v", err)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := client.Database("mdb_test_objectid")
	defer db.DropCollection(ctx, "oid_collection")

	coll := db.Collection("oid_collection")

	// Insert with explicit ObjectID
	oid := ObjectID()
	_, err = coll.InsertOne(ctx, Document{
		"_id":  oid,
		"name": "with_oid",
	})
	if err != nil {
		t.Fatalf("InsertOne with ObjectID failed: %v", err)
	}

	// Find by ObjectID
	result := Document{}
	sr, err := coll.FindOne(ctx, Document{"_id": oid})
	if err != nil {
		t.Fatalf("FindOne by ObjectID failed: %v", err)
	}
	err = sr.Decode(&result)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if result["name"] != "with_oid" {
		t.Errorf("expected name 'with_oid', got %v", result["name"])
	}
}

func TestIntegrationCollectionStats(t *testing.T) {
	client, err := NewClient("localhost", 27017)
	if err != nil {
		t.Skipf("Skipping collection stats tests: cannot connect to MDB: %v", err)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := client.Database("mdb_test_stats")
	defer db.DropCollection(ctx, "stats_collection")

	coll := db.Collection("stats_collection")

	// Insert some data
	for i := 0; i < 10; i++ {
		_, err := coll.InsertOne(ctx, Document{"index": i, "value": ObjectID()})
		if err != nil {
			t.Fatalf("InsertOne failed: %v", err)
		}
	}

	// Collection stats
	stats, err := coll.Stats(ctx)
	if err != nil {
		t.Fatalf("Collection Stats failed: %v", err)
	}
	if stats == nil {
		t.Error("expected non-nil collection stats")
	}

	// Validate
	validateResult, err := coll.Validate(ctx)
	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}
	if validateResult == nil {
		t.Error("expected non-nil validate result")
	}
}

func TestIntegrationRunCommand(t *testing.T) {
	client, err := NewClient("localhost", 27017)
	if err != nil {
		t.Skipf("Skipping run command tests: cannot connect to MDB: %v", err)
	}
	defer client.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db := client.Database("test")

	// Ping command
	result, err := db.RunCommand(ctx, Document{"ping": 1})
	if err != nil {
		t.Fatalf("RunCommand ping failed: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil ping result")
	}
}
