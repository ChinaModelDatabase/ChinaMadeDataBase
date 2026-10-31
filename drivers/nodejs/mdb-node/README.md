# mdb-node

Node.js driver for MDB - Xinchuang Document Database.

MDB is wire-protocol compatible with MongoDB 7.0. This driver wraps the official `mongodb` npm package with MDB-specific extensions.

## Installation

```bash
npm install mdb-node
```

## Quick Start

```typescript
import { MdbClient } from "mdb-node";

// Connect to MDB
const client = MdbClient.create("localhost", 27017);
await client.connect();

// Get database and collection
const db = client.database("mydb");
const users = db.collection("users");

// Insert documents
await users.insertOne({ name: "Alice", age: 30 });
await users.insertMany([
  { name: "Bob", age: 25 },
  { name: "Charlie", age: 35 }
]);

// Query documents
const results = await users.find({ age: { $gte: 18 } })
  .sort({ name: 1 })
  .limit(10)
  .toArray();

console.log(results);

// Update documents
await users.updateOne(
  { name: "Alice" },
  { $set: { age: 31 } }
);

// Delete documents
await users.deleteOne({ name: "Bob" });

// Close connection
await client.close();
```

## Connection Options

```typescript
// From URI
const client = MdbClient.fromURI("mongodb://host:27017/mydb");

// With host and port
const client = MdbClient.create("localhost", 27017);

// With authentication
const client = MdbClient.createWithAuth(
  "localhost", 27017,
  "admin", "secret",
  "admin" // auth database
);

// With options
const client = MdbClient.create("localhost", 27017, {
  appName: "myapp",
  replicaSet: "rs0",
  maxPoolSize: 10,
  connectTimeoutMS: 5000,
});
```

## Async Dispose (ES2022+)

```typescript
using client = MdbClient.create("localhost", 27017);
const db = client.database("mydb");
// client is automatically closed when leaving scope
```

## Transactions

```typescript
const txn = await client.startTransaction();
try {
  const db = txn.database("mydb");
  const accounts = db.collection("accounts");

  await accounts.updateOne(
    { _id: "account1" },
    { $inc: { balance: -100 } }
  );
  await accounts.updateOne(
    { _id: "account2" },
    { $inc: { balance: 100 } }
  );

  await txn.commit();
} catch (error) {
  await txn.abort();
  throw error;
}
```

## Change Streams

```typescript
const changeStream = users.watch([
  { $match: { "operationType": "insert" } }
]);

for await (const change of changeStream) {
  console.log("New user:", change.fullDocument);
}
```

## GridFS (Large Files)

```typescript
const bucket = db.collection("uploads").gridfsBucket();

// Upload from buffer
const fileId = await bucket.uploadFromBytes(
  "photo.jpg",
  imageData,
  { contentType: "image/jpeg" }
);

// Download to buffer
const data = await bucket.downloadToBytes(fileId);

// Upload from stream
import { createReadStream } from "fs";
const stream = createReadStream("./video.mp4");
const videoId = await bucket.uploadFromStream("video.mp4", stream);
```

## Query Profiling

```typescript
const profiler = db.profiler();

// Enable profiling (queries > 100ms)
await profiler.enable(100);

// Get slow queries
const slowQueries = await profiler.slowQueries(50);

// Get profiler stats
const stats = await profiler.stats();

// Disable profiling
await profiler.disable();
```

## Index Management

```typescript
import { ascending, descending, compoundIndex, ttlIndex } from "mdb-node";

// Create indexes
await users.createIndex(ascending("email"), { unique: true });
await users.createIndex(compoundIndex(["lastName", 1], ["firstName", 1]));
await users.createIndex(ttlIndex("expiresAt", 3600));

// List indexes
const indexes = await users.listIndexes();
```

## User Management

```typescript
const db = client.database("mydb");

// Create user
await db.createUser("appuser", "securepassword", [
  { role: "readWrite", db: "mydb" }
]);

// Get all users
const users = await db.getUsers();

// Drop user
await db.dropUser("appuser");
```

## Write & Read Concerns

```typescript
import { WriteConcern, ReadConcern } from "mdb-node";

// Majority write concern
const wc = WriteConcern.MAJORITY;

// Linearizable read concern
const rc = ReadConcern.LINEARIZABLE;

// Use in operations
await users.insertOne(
  { name: "test" },
  { writeConcern: wc.toDocument() }
);
```

## License

MIT
