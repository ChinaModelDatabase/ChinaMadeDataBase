/**
 * MDB Node.js Driver
 *
 * A Node.js driver for MDB - Xinchuang Document Database.
 * MDB is wire-protocol compatible with MongoDB 7.0,
 * so this driver wraps the official mongodb npm package with MDB-specific extensions.
 *
 * @example
 * ```typescript
 * import { MdbClient, MdbCollection } from "mdb-node";
 *
 * // Connect to MDB
 * const client = MdbClient.create("localhost", 27017);
 * await client.connect();
 *
 * // Get database and collection
 * const db = client.database("mydb");
 * const users = db.collection("users");
 *
 * // CRUD operations
 * await users.insertOne({ name: "Alice", age: 30 });
 * const result = await users.find({ age: { $gte: 18 } }).toArray();
 *
 * // Clean up
 * await client.close();
 * ```
 *
 * @packageDocumentation
 */

// Core classes
export { MdbClient } from "./client";
export { MdbDatabase } from "./database";
export { MdbCollection, OptionalId } from "./collection";
export { MdbCursor } from "./cursor";
export { MdbChangeStream } from "./change_stream";
export { MdbSession, MdbTransaction } from "./transaction";
export { MdbGridFSBucket } from "./gridfs";
export { MdbProfiler } from "./profiler";

// Concerns
export { WriteConcern, ReadConcern } from "./concerns";

// Index helpers
export {
  ASC,
  DESC,
  GEO2D,
  GEOSPHERE,
  HASHED,
  TEXT,
  ascending,
  descending,
  compoundIndex,
  textIndex,
  geo2dIndex,
  geoSphereIndex,
  hashedIndex,
  ttlIndex,
  uniqueIndex,
  sparseIndex,
  partialFilter,
  buildIndexName,
  MdbIndexes,
} from "./index_helpers";

// Types
export {
  MdbDocument,
  Filter,
  Update,
  Sort,
  Projection,
  IndexSpecification,
  ClientOptions,
  FindOptions,
  InsertOneResult,
  InsertManyResult,
  UpdateResult,
  DeleteResult,
  SessionOptions,
  TransactionOptions,
  CreateCollectionOptions,
  CreateIndexOptions,
  AggregateOptions,
  UpdateOptions,
  ReplaceOptions,
  WatchOptions,
  ChangeStreamDocument,
  GridFSBucketOptions,
  GridFSFile,
  UploadOptions,
  Role,
  ProfilerLevel,
  HeartbeatData,
} from "./types";
