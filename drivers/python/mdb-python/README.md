# MDB Python Driver

Python driver for MDB (Xinchuang Document Database). MDB is wire-protocol compatible with MongoDB 7.0, so this driver wraps pymongo with MDB-specific extensions.

## Installation

```bash
pip install -e .
```

## Quick Start

```python
from mdb import MdbClient

# Connect to MDB
client = MdbClient("localhost", 27017)

# Or use a URI
client = MdbClient.from_uri("mongodb://localhost:27017")

# Get a database
db = client.database("mydb")

# Get a collection
coll = db.collection("users")

# Insert documents
coll.insert_one({"name": "Alice", "email": "alice@example.com"})
coll.insert_many([
    {"name": "Bob", "email": "bob@example.com"},
    {"name": "Charlie", "email": "charlie@example.com"},
])

# Query documents
user = coll.find_one({"name": "Alice"})
users = coll.find({"name": {"$ne": "Alice"}}).to_list()

# Update documents
coll.update_one({"name": "Alice"}, {"$set": {"email": "alice@new.com"}})
coll.update_many({"name": {"$exists": True}}, {"$set": {"active": True}})

# Delete documents
coll.delete_one({"name": "Charlie"})
coll.delete_many({"active": False})

# Aggregation
pipeline = [
    {"$group": {"_id": "$active", "count": {"$sum": 1}}},
]
results = coll.aggregate(pipeline).to_list()

# Create indexes
coll.create_index([("name", 1)], unique=True)
coll.create_index([("email", 1)])

# Transaction support
with client.start_transaction() as txn:
    coll = txn.collection("mydb", "users")
    coll.insert_one({"name": "Dave", "email": "dave@example.com"})

# GridFS
bucket = coll.gridfs_bucket()
file_id = bucket.upload_from_bytes("document.pdf", pdf_bytes)
data = bucket.download_to_bytes(file_id)

# Context manager
with MdbClient("localhost", 27017) as client:
    db = client.database("mydb")
    # ...
```

## API Reference

### MdbClient

```python
from mdb import MdbClient

# Create client
client = MdbClient(host, port, username=None, password=None, auth_source="admin", **kwargs)

# From URI
client = MdbClient.from_uri(uri)

# Get database
db = client.database("name")
db = client["name"]

# List databases
dbs = client.list_databases()

# Server status
status = client.server_status()

# Replica set status
rs_status = client.replica_set_status()

# Check connection
connected = client.is_connected()

# Start transaction
txn = client.start_transaction()

# Close
client.close()
```

### MdbDatabase

```python
db = client.database("mydb")

# Get collection
coll = db.collection("users")
coll = db["users"]

# List collections
colls = db.list_collections()

# Create collection
db.create_collection("logs", capped=True, size=1048576)

# Drop collection
db.drop_collection("logs")

# Database stats
stats = db.stats()

# Profiler
profiler = db.profiler()
profiler.enable(slow_ms=100)
profiler.disable()

# User management
db.create_user("admin", "password", roles=[{"role": "readWrite", "db": "mydb"}])
db.drop_user("admin")
users = db.get_users()
```

### MdbCollection

```python
coll = db.collection("users")

# Find
cursor = coll.find({"age": {"$gte": 18}}, projection={"name": 1, "age": 1})
doc = coll.find_one({"_id": oid})

# Insert
result = coll.insert_one({"name": "Alice"})
result = coll.insert_many([{"name": "Bob"}, {"name": "Charlie"}])

# Update
result = coll.update_one({"name": "Alice"}, {"$set": {"age": 30}})
result = coll.update_many({"age": {"$lt": 18}}, {"$set": {"status": "minor"}})

# Delete
result = coll.delete_one({"name": "Alice"})
result = coll.delete_many({"status": "inactive"})

# Replace
result = coll.replace_one({"name": "Alice"}, {"name": "Alice", "age": 30})

# Find and modify
doc = coll.find_one_and_update({"name": "Alice"}, {"$inc": {"age": 1}})
doc = coll.find_one_and_replace({"name": "Bob"}, {"name": "Bob", "age": 25})
doc = coll.find_one_and_delete({"name": "Charlie"})

# Aggregation
results = coll.aggregate([
    {"$group": {"_id": "$status", "count": {"$sum": 1}}},
]).to_list()

# Distinct
values = coll.distinct("status")

# Count
count = coll.count_documents({"status": "active"})

# Indexes
coll.create_index([("name", 1)], unique=True)
coll.create_index([("email", 1)])
coll.drop_index("name_1")
indexes = coll.list_indexes()
info = coll.index_information()

# Stats and maintenance
stats = coll.stats()
coll.validate()
coll.compact()
coll.re_index()

# Change streams
with coll.watch() as stream:
    for event in stream:
        print(event.operation_type)

# GridFS
bucket = coll.gridfs_bucket()
```

### MdbCursor

```python
cursor = coll.find({"status": "active"})

# Chaining
results = (
    coll.find()
    .sort("name", 1)
    .limit(10)
    .skip(0)
    .batch_size(100)
    .projection({"name": 1})
    .to_list()
)

# Iterator
for doc in cursor:
    print(doc)

# Properties
print(cursor.alive)

# Explain
plan = coll.find({"name": "Alice"}).explain()

# Context manager
with coll.find() as cursor:
    for doc in cursor:
        print(doc)
```

### MdbChangeStream

```python
with coll.watch() as stream:
    for event in stream:
        print(event.operation_type)
        print(event.full_document)
        print(event.document_key)
        print(event.ns)

# Resume from token
stream.resume_after(token)
stream.start_after(token)
stream.start_at_operation_time(time)

# Filter pipeline
stream.pipeline([{"$match": {"operationType": "insert"}}])

# Try next (non-blocking)
event = stream.try_next()
```

### MdbTransaction

```python
with client.start_transaction() as txn:
    db = txn.database("mydb")
    coll = txn.collection("mydb", "accounts")
    
    coll.update_one({"_id": "account1"}, {"$inc": {"balance": -100}})
    coll.update_one({"_id": "account2"}, {"$inc": {"balance": 100}})

# Manual transaction control
txn = client.start_transaction()
txn.start()
try:
    coll = txn.collection("mydb", "accounts")
    coll.update_one({"_id": "account1"}, {"$inc": {"balance": -100}})
    txn.commit()
except Exception:
    txn.abort()
```

### MdbGridFSBucket

```python
bucket = coll.gridfs_bucket()

# Upload
file_id = bucket.upload_from_bytes("doc.pdf", pdf_bytes)
file_id = bucket.upload_from_file("doc.pdf", file_obj)

# Download
data = bucket.download_to_bytes(file_id)
bucket.download_to_file(file_id, file_obj)

# List files
files = bucket.find({"filename": "doc.pdf"})

# Delete
bucket.delete(file_id)

# Rename
bucket.rename(file_id, "new_name.pdf")
```

### MdbProfiler

```python
profiler = db.profiler()

# Enable/disable
profiler.enable(slow_ms=100)
profiler.disable()

# Get slow queries
queries = profiler.slow_queries(limit=100)

# Stats
stats = profiler.stats()

# Set level (0=off, 1=slow, 2=all)
profiler.set_level(2)

# Reset
profiler.reset()
```

### Index Helpers

```python
from mdb import MdbIndex, ASCENDING, DESCENDING, GEOSPHERE, TEXT, HASHED

# Create index with helper
index = MdbIndex([("name", ASCENDING)], unique=True)
coll.create_index([("name", ASCENDING)], unique=True)

# Types
# ASCENDING = 1
# DESCENDING = -1
# GEOSPHERE = "2dsphere"
# TEXT = "text"
# HASHED = "hashed"
```

### WriteConcern

```python
from mdb import WriteConcern, WValue

# Majority write concern
wc = WriteConcern(w=WValue.MAJORITY)

# Custom
wc = WriteConcern(w=1, wtimeout_ms=1000, j=True)

# Use with collection
coll = db.collection("users", write_concern=wc)
```

### ReadConcern

```python
from mdb import ReadConcern, ReadConcernLevel

# Majority read concern
rc = ReadConcern(level=ReadConcernLevel.MAJORITY)

# Use with collection
coll = db.collection("users", read_concern=rc)
```

## Testing

```bash
# Run tests
pytest tests/ -v

# Run with MDB instance
MDB_HOST=localhost MDB_PORT=27017 pytest tests/ -v
```

## License

MIT
