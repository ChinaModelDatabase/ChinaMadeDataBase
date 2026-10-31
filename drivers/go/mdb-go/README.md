# mdb-go

Go driver for MDB (Xinchuang Document Database). MDB is wire-protocol compatible with MongoDB 7.0, so this driver wraps the official [mongo-go-driver](https://github.com/mongodb/mongo-go-driver) with MDB-specific extensions.

## Installation

```bash
go get github.com/mdb-official/mdb-go
```

## Quick Start

### Basic Connection

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/mdb-official/mdb-go"
)

func main() {
    client, err := mdb.NewClient("localhost", 27017)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close(context.Background())

    db := client.Database("mydb")
    coll := db.Collection("mycollection")

    // Insert a document
    result, err := coll.InsertOne(context.Background(), mdb.BSON{
        "name": "Alice",
        "age":  30,
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Inserted ID:", result.InsertedID)
}
```

### Connection with Authentication

```go
client, err := mdb.NewClientWithAuth("localhost", 27017, "admin", "password", "admin")
if err != nil {
    log.Fatal(err)
}
defer client.Close(context.Background())
```

### URI Connection

```go
client, err := mdb.NewClientFromURI("mongodb://localhost:27017/mydb?authSource=admin")
if err != nil {
    log.Fatal(err)
}
defer client.Close(context.Background())
```

### Client Options

```go
client, err := mdb.NewClient("localhost", 27017,
    mdb.WithAppName("my-app"),
    mdb.WithMaxPoolSize(100),
    mdb.WithMinPoolSize(10),
    mdb.WithTimeout(5*time.Second),
    mdb.WithWriteConcern(mdb.WriteConcernMajority),
    mdb.WithReadConcern(mdb.ReadConcernMajority),
)
```

## CRUD Operations

### Insert

```go
// Insert one
result, err := coll.InsertOne(ctx, mdb.BSON{
    "name": "Alice",
    "age":  30,
})

// Insert many
docs := []interface{}{
    mdb.BSON{"name": "Bob", "age": 25},
    mdb.BSON{"name": "Charlie", "age": 35},
}
result, err := coll.InsertMany(ctx, docs)
```

### Find

```go
// Find one
sr, err := coll.FindOne(ctx, mdb.BSON{"name": "Alice"})
var doc mdb.BSON
err = sr.Decode(&doc)

// Find with options
cursor, err := coll.Find(ctx, mdb.BSON{
    "age": mdb.BSON{"$gte": 25},
}, &mdb.FindOptions{
    Limit: 10,
    Sort:  mdb.Descending("age"),
})
var results []mdb.BSON
err = cursor.All(ctx, &results)
```

### Update

```go
// Update one
result, err := coll.UpdateOne(ctx,
    mdb.BSON{"name": "Alice"},
    mdb.BSON{"$set": mdb.BSON{"age": 31}},
)

// Update many
result, err := coll.UpdateMany(ctx,
    mdb.BSON{"age": mdb.BSON{"$gte": 30}},
    mdb.BSON{"$set": mdb.BSON{"status": "senior"}},
)
```

### Delete

```go
// Delete one
result, err := coll.DeleteOne(ctx, mdb.BSON{"name": "Alice"})

// Delete many
result, err := coll.DeleteMany(ctx, mdb.BSON{"age": mdb.BSON{"$lt": 25}})
```

## Aggregation

```go
pipeline := mdb.BSON{
    mdb.BSON{"$match": mdb.BSON{"status": "active"}},
    mdb.BSON{"$group": mdb.BSON{
        "_id":   "$department",
        "count": mdb.BSON{"$sum": 1},
    }},
}

cursor, err := coll.Aggregate(ctx, pipeline)
var results []mdb.BSON
err = cursor.All(ctx, &results)
```

## Indexes

```go
// Create ascending index
name, err := coll.CreateIndex(ctx, mdb.Ascending("name"))

// Create compound index
name, err = coll.CreateIndex(ctx, mdb.Ascending("lastname", "firstname"))

// Create unique sparse index
name, err = coll.CreateIndex(ctx, mdb.Ascending("email"), &mdb.IndexOptions{
    Unique: true,
    Sparse: true,
})

// Create text index
name, err = coll.CreateIndex(ctx, mdb.TextIndex("title", "body"))

// List indexes
cursor, err := coll.ListIndexes(ctx)

// Drop index
err = coll.DropIndex(ctx, "name_1")
```

## Transactions

```go
tx, err := client.StartTransaction()
if err != nil {
    log.Fatal(err)
}

db := tx.Database("mydb")
coll := db.Collection("accounts")

_, err = coll.UpdateOne(ctx, mdb.BSON{"_id": "alice"}, mdb.BSON{"$inc": mdb.BSON{"balance": -100}})
if err != nil {
    tx.Abort()
    log.Fatal(err)
}

_, err = coll.UpdateOne(ctx, mdb.BSON{"_id": "bob"}, mdb.BSON{"$inc": mdb.BSON{"balance": 100}})
if err != nil {
    tx.Abort()
    log.Fatal(err)
}

err = tx.Commit()
if err != nil {
    log.Fatal(err)
}
```

### Using WithTransaction

```go
result, err := client.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
    db := client.Database("mydb")
    coll := db.Collection("accounts")

    _, err := coll.UpdateOne(sessCtx, mdb.BSON{"_id": "alice"}, mdb.BSON{"$inc": mdb.BSON{"balance": -100}})
    if err != nil {
        return nil, err
    }

    _, err = coll.UpdateOne(sessCtx, mdb.BSON{"_id": "bob"}, mdb.BSON{"$inc": mdb.BSON{"balance": 100}})
    if err != nil {
        return nil, err
    }

    return nil, nil
})
```

## Change Streams

```go
cursor, err := coll.Watch(ctx, nil)
if err != nil {
    log.Fatal(err)
}
defer cursor.Close(ctx)

for cursor.Next(ctx) {
    var event mdb.BSON
    err := cursor.Decode(&event)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Change event:", event)
}
```

## GridFS

```go
bucket := coll.GridFSBucket(&mdb.GridFSBucketOptions{
    Name:      "uploads",
    ChunkSize: 255 * 1024,
})

// Upload
fileID, err := bucket.UploadFromBytes("document.pdf", data)

// Download
data, err := bucket.DownloadToBytes(fileID)

// Find files
files, err := bucket.Find(mdb.BSON{"filename": "document.pdf"})

// Rename
err = bucket.Rename(fileID, "renamed.pdf")

// Delete
err = bucket.Delete(fileID)
```

## Database Operations

```go
db := client.Database("mydb")

// List collections
collections, err := db.ListCollections(ctx)

// Create collection
err = db.CreateCollection(ctx, "new_collection")

// Drop collection
err = db.DropCollection(ctx, "old_collection")

// Database stats
stats, err := db.Stats(ctx)

// Run command
result, err := db.RunCommand(ctx, mdb.BSON{"ping": 1})
```

## Server Operations

```go
// Server status
status, err := client.ServerStatus(ctx)

// Replica set status
rsStatus, err := client.ReplicaSetStatus(ctx)

// List databases
databases, err := client.ListDatabases(ctx)
```

## User Management

```go
db := client.Database("mydb")

// Create user
err = db.CreateUser(ctx, "newuser", "password", []mdb.Role{
    {Role: "readWrite", DB: "mydb"},
    {Role: "read", DB: "otherdb"},
})

// Drop user
err = db.DropUser(ctx, "newuser")
```

## Collection Operations

```go
coll := db.Collection("mycollection")

// Collection stats
stats, err := coll.Stats(ctx)

// Validate collection
result, err := coll.Validate(ctx)

// Compact collection
err = coll.Compact(ctx)

// Reindex collection
err = coll.ReIndex(ctx)
```

## Concern Levels

```go
// Write concerns
mdb.WriteConcernAcknowledged  // Acknowledged
mdb.WriteConcernMajority      // Majority

// Read concerns
mdb.ReadConcernLocal          // Local
mdb.ReadConcernMajority       // Majority
mdb.ReadConcernLinearizable   // Linearizable
mdb.ReadConcernSnapshot       // Snapshot
```

## License

MIT License
