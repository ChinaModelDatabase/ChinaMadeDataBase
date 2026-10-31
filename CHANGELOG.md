# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-10-03

### Added

#### Storage Engine (mdb-storage)
- **LRU Block Cache**: 256MB block cache with LRU eviction policy
- **TDE (Transparent Data Encryption)**: SSTable data block encryption/decryption
  - `EncryptionManager` with MEK→DEK key hierarchy
  - `EncryptionContext` for SSTable writer/reader
  - `encryptCollection`/`decryptCollection`/`keyStatus`/`rotateKey` commands

#### Query Engine (mdb-server)
- **Index usage in read path**: Query planner + index lookup
  - `plan_query()` analyzes filter conditions
  - `execute_indexed_query()` uses index or falls back to full scan
  - `explain` reports real execution plans (COLLSCAN/IXSCAN)
  - Index recovery from Catalog on restart
- **New query operators**: $nin, $regex, $elemMatch, $mod, $type, $size, $all, $not, $nor
- **Dotted path support**: `{"address.city": "北京"}` now works
- **Enhanced bson_eq/bson_compare**: Support for DateTime, Timestamp, Binary, Document, Array
- **Array element comparison**: $gt/$gte/$lt/$lte work on array values
- **Aggregation stages**: $lookup, $bucket, $sample, $facet, $setWindowFields
- **MapReduce**: Simplified implementation using aggregation pipeline

#### Index Types (mdb-server)
- **TTL index**: `expireAfterSeconds` support
- **Text index**: `{"key": {"content": "text"}}`
- **Hashed index**: `{"key": {"email": "hashed"}}`
- **2dsphere index**: `{"key": {"location": "2dsphere"}}`
- **Partial index**: `partialFilterExpression` support

#### Transactions (mdb-server)
- **Real transaction semantics**: startTransaction/commitTransaction/abortTransaction
- **Write buffering**: Operations buffered during transaction, applied on commit
- **Rollback**: abortTransaction discards buffered operations

#### Change Stream (mdb-server)
- **watch command**: Returns real events from oplog
- **Resume token**: Supports resumption via postBatchResumeToken
- **Oplog integration**: Write paths automatically append to oplog

#### Sharding (mdb-server + mdb-router)
- **Data routing**: Insert/Find/Update/Delete automatically routed
  - `_shard` field tracks shard assignment
  - Targeted queries filter by shard
  - Broadcast queries return all shards
- **Chunk migration**: `moveChunk` with real data migration
  - Reads documents in chunk range
  - Updates `_shard` field to destination
  - Updates ConfigDb metadata
- **Migration lock**: Write buffering during migration
- **Migration state machine**: 7 states (Preparing→Cloning→Persisting→Committing→Cleanup→Completed/Aborted)
- **ConfigDb persistence**: Sharding metadata stored in Catalog
- **Sharding commands**: addShard, removeShard, listShards, enableSharding, shardCollection, splitAt, moveChunk, balancerStart/Stop/Status, flushRouterConfig

#### TLS (mdb-server)
- **Standard TLS 1.2/1.3**: rustls integration
  - `RustlsStream` implements Read/Write for Framed
  - Dual-stack dispatch (TLCP/rustls)
  - Certificate loading from `<dbpath>/tls/`
- **TLCP fallback**: mdb-tls (SM4-GCM) as fallback

#### Replica Set (mdb-server)
- **ReplState**: Raft state (term/role) + ReplicaSet + config
- **replSetInitiate**: Parses member config, creates Raft state
- **replSetGetStatus**: Reports real Raft state
- **replSetGetConfig**: Returns stored config document
- **replSetReconfig**: Leader executes config changes

#### Operations (mdb-server)
- **Profiler**: Records operations with timing
  - Level 0/1/2 (off/slow/all)
  - Slow query threshold
  - system.profile query support
- **setParameter/getParameter**: Runtime parameter adjustment
- **collMod**: Collection modification (TTL, validator)
- **dbHash**: Data checksum (FNV-1a)
- **OP_COMPRESSED**: Snappy decompression support
- **wireVersion upgrade**: From 21 (MongoDB 7.0) to 25 (MongoDB 8.0)

#### CLI (mdb-cli)
- **Interactive shell**: REPL connecting to mdb-server
- **MongoDB command support**: Raw JSON commands
- **Connection string support**: `mongodb://` format

#### Wire Protocol (mdb-wire)
- **OP_COMPRESSED**: Snappy decompression for compressed messages
- **wireVersion 25**: MongoDB 8.0 compatibility

### Changed

- **Internal key encoding**: Order-preserving escape (`0x00→0x01 0x01, 0x01→0x01 0x02`)
- **SST/MemTable comparisons**: Unified escape-compare-unescape path
- **update/delete shorthand**: Support MongoDB filter-first format
- **createIndexes**: Persist index definitions to Catalog
- **Server::bind_with_dbpath**: New method for persistent mode
- **Bloom filter**: Uses raw user keys for build/probe
- **tls_enabled**: Changed from `bool` to `AtomicBool`
- **State wiring**: `Server::with_tls` syncs `state.tls_enabled`

### Fixed

- **Internal key NUL handling**: Keys with embedded `0x00` now work correctly
- **Index recovery**: Index definitions loaded from Catalog on restart
- **Empty file GridFS**: Empty files return 0 chunks (not 1 empty chunk)
- **Wire version tests**: Updated to reflect new wireVersion 25

## [0.1.0] - 2026-09-30

### Added

#### Storage Engine (mdb-storage)
- LSM-Tree architecture with WAL + MemTable + SSTable
- Group commit for WAL (batch fsync)
- LRU Block Cache (256MB)
- LZ4 and Snappy block compression
- Bloom filter for negative lookups
- Leveled compaction (L0 → Ln)
- CRC32C checksums per block
- Snapshot isolation with MVCC

#### Query Engine (mdb-query)
- 20+ query operators ($eq, $ne, $gt, $lt, $in, $regex, etc.)
- 20+ aggregation pipeline stages ($match, $group, $sort, $lookup, $graphLookup, $setWindowFields, $densify, $sample, $redact, etc.)
- Regex engine with Unicode, \d\w\s, {n,m} quantifiers, lazy quantifiers
- Explain plan support
- Query profiler with slow query logging
- Cursor management with tailable cursors

#### Index (mdb-index)
- B+Tree with RwLock concurrency
- Binary search in internal nodes
- Prefix compression
- Bulk loading (O(n) bottom-up)
- Unique, TTL, Text, 2dsphere, Hashed indexes
- Index statistics

#### Transactions (mdb-txn)
- MVCC with snapshot isolation
- Write/Read Concern (Acknowledged, Majority, NumAck, Tag)
- Retryable writes with deduplication
- Causal consistency sessions
- Read/Write preferences

#### Replication (mdb-repl)
- Raft consensus with snapshots
- PreVote optimization
- Joint consensus membership changes
- WAL persistence for Raft log
- Cursor-based oplog replication
- Change Stream with pipeline filter, fullDocument, resume tokens

#### Sharding (mdb-router)
- Hashed/Range/Compound shard keys
- Config server for metadata
- Chunk balancer with migration state machine
- Shard key validation

#### Server (mdb-server)
- 30+ MongoDB-compatible commands
- SCRAM-SHA-256 authentication
- Connection pool with work-stealing thread pool
- 7 new update operators ($each, $position, $slice, $bit, $pullAll, $[], $[identifier])
- GridFS support
- Graceful shutdown

#### Drivers
- Java self-contained driver (BSON + Wire Protocol)
- Python self-contained driver
- Go self-contained driver
- Node.js self-contained driver
- C/C++ self-contained driver

#### GUI
- MDB Compass (Electron + React + TypeScript)
- Connection management
- Database tree browser
- Document viewer/editor
- Query builder
- Index manager
- Aggregation pipeline builder
- Performance monitoring charts

#### Testing
- 1,110 tests passing
- Zero unsafe code (enforced by mdb-test)
- Zero external runtime dependencies

### Security
- SCRAM-SHA-256 authentication
- SM3/SM4 national cryptography support
- MLPS Level 3 compliance design
- No unsafe Rust code anywhere in codebase
