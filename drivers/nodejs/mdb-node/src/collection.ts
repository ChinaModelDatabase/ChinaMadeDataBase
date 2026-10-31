import {
  Collection,
  Document,
  ClientSession,
  GridFSBucket,
  ObjectId,
} from "mongodb";
import {
  Filter,
  Update,
  FindOptions,
  InsertOneResult,
  InsertManyResult,
  UpdateResult,
  DeleteResult,
  Sort,
  Projection,
  AggregateOptions,
  UpdateOptions,
  ReplaceOptions,
  IndexSpecification,
  CreateIndexOptions,
  WatchOptions,
  GridFSBucketOptions,
} from "./types";
import { MdbDatabase } from "./database";
import { MdbCursor } from "./cursor";
import { MdbChangeStream } from "./change_stream";
import { MdbGridFSBucket } from "./gridfs";

/** Helper type to make _id optional for insert operations. */
export type OptionalId<T> = Omit<T, "_id"> & { _id?: any };

/**
 * MDB Collection - provides CRUD operations and administrative methods.
 *
 * @example
 * ```typescript
 * const db = client.database("mydb");
 * const users = db.collection("users");
 *
 * // Insert
 * await users.insertOne({ name: "Alice", age: 30 });
 * await users.insertMany([{ name: "Bob" }, { name: "Charlie" }]);
 *
 * // Query
 * const cursor = users.find({ age: { $gte: 18 } }).sort({ name: 1 });
 * const results = await cursor.toArray();
 *
 * // Update
 * await users.updateOne({ name: "Alice" }, { $set: { age: 31 } });
 *
 * // Delete
 * await users.deleteOne({ name: "Bob" });
 * ```
 */
export class MdbCollection {
  private _db: MdbDatabase;
  private _collection: Collection;
  private _session?: ClientSession;

  /** @internal */
  constructor(db: MdbDatabase, collection: Collection, session?: ClientSession) {
    this._db = db;
    this._collection = collection;
    this._session = session;
  }

  /** The collection name. */
  get name(): string {
    return this._collection.collectionName;
  }

  /** The underlying MongoDB Collection instance. */
  get native(): Collection {
    return this._collection;
  }

  /** The parent database. */
  get database(): MdbDatabase {
    return this._db;
  }

  /**
   * Find documents matching a filter.
   * @param filter - Query filter.
   * @param options - Find options.
   * @returns A cursor for iterating over results.
   */
  find<T = Document>(filter?: Filter<T>, options?: FindOptions): MdbCursor<T> {
    const cursor = this._collection.find(filter as any, {
      ...options,
      session: this._session,
    } as any);
    return new MdbCursor<T>(cursor as any);
  }

  /**
   * Find a single document matching a filter.
   * @param filter - Query filter.
   * @param projection - Fields to include/exclude.
   * @returns The matching document or null.
   */
  async findOne<T = Document>(
    filter?: Filter<T>,
    projection?: Projection
  ): Promise<T | null> {
    const result = await this._collection.findOne(filter as any, {
      projection: projection as any,
      session: this._session,
    });
    return result as T | null;
  }

  /**
   * Insert a single document.
   * @param doc - The document to insert.
   * @returns Insert result with the inserted ID.
   */
  async insertOne<T = Document>(doc: OptionalId<T>): Promise<InsertOneResult> {
    const result = await this._collection.insertOne(doc as any, {
      session: this._session,
    } as any);
    return {
      insertedId: result.insertedId,
      acknowledged: result.acknowledged,
    };
  }

  /**
   * Insert multiple documents.
   * @param docs - Array of documents to insert.
   * @returns Insert result with inserted IDs.
   */
  async insertMany<T = Document>(docs: OptionalId<T>[]): Promise<InsertManyResult> {
    const result = await this._collection.insertMany(docs as any[], {
      session: this._session,
    } as any);
    return {
      insertedIds: result.insertedIds as Record<number, ObjectId>,
      insertedCount: result.insertedCount,
      acknowledged: result.acknowledged,
    };
  }

  /**
   * Update a single document matching a filter.
   * @param filter - Query filter.
   * @param update - Update operations.
   * @param options - Update options.
   * @returns Update result.
   */
  async updateOne<T = Document>(
    filter: Filter<T>,
    update: Update<T>,
    options?: UpdateOptions
  ): Promise<UpdateResult> {
    const result = await this._collection.updateOne(
      filter as any,
      update as any,
      { ...options, session: this._session } as any
    );
    return {
      matchedCount: result.matchedCount,
      modifiedCount: result.modifiedCount,
      upsertedId: result.upsertedId as ObjectId | null,
      upsertedCount: result.upsertedCount ?? 0,
      acknowledged: result.acknowledged,
    };
  }

  /**
   * Update multiple documents matching a filter.
   * @param filter - Query filter.
   * @param update - Update operations.
   * @param options - Update options.
   * @returns Update result.
   */
  async updateMany<T = Document>(
    filter: Filter<T>,
    update: Update<T>,
    options?: UpdateOptions
  ): Promise<UpdateResult> {
    const result = await this._collection.updateMany(
      filter as any,
      update as any,
      { ...options, session: this._session } as any
    );
    return {
      matchedCount: result.matchedCount,
      modifiedCount: result.modifiedCount,
      upsertedId: result.upsertedId as ObjectId | null,
      upsertedCount: result.upsertedCount ?? 0,
      acknowledged: result.acknowledged,
    };
  }

  /**
   * Delete a single document matching a filter.
   * @param filter - Query filter.
   * @returns Delete result.
   */
  async deleteOne<T = Document>(filter: Filter<T>): Promise<DeleteResult> {
    const result = await this._collection.deleteOne(filter as any, {
      session: this._session,
    } as any);
    return {
      deletedCount: result.deletedCount,
      acknowledged: result.acknowledged,
    };
  }

  /**
   * Delete multiple documents matching a filter.
   * @param filter - Query filter.
   * @returns Delete result.
   */
  async deleteMany<T = Document>(filter: Filter<T>): Promise<DeleteResult> {
    const result = await this._collection.deleteMany(filter as any, {
      session: this._session,
    } as any);
    return {
      deletedCount: result.deletedCount,
      acknowledged: result.acknowledged,
    };
  }

  /**
   * Replace an entire document matching a filter.
   * @param filter - Query filter.
   * @param replacement - The replacement document (without _id).
   * @param options - Replace options.
   * @returns Update result.
   */
  async replaceOne<T = Document>(
    filter: Filter<T>,
    replacement: Omit<T, "_id">,
    options?: ReplaceOptions
  ): Promise<UpdateResult> {
    const result = await this._collection.replaceOne(
      filter as any,
      replacement as any,
      { ...options, session: this._session } as any
    );
    return {
      matchedCount: result.matchedCount,
      modifiedCount: result.modifiedCount,
      upsertedId: result.upsertedId as ObjectId | null,
      upsertedCount: result.upsertedCount ?? 0,
      acknowledged: result.acknowledged,
    };
  }

  /**
   * Perform an aggregation pipeline.
   * @param pipeline - Array of aggregation stages.
   * @param options - Aggregate options.
   * @returns A cursor for iterating over aggregation results.
   */
  aggregate<T = Document>(
    pipeline: Document[],
    options?: AggregateOptions
  ): MdbCursor<T> {
    const cursor = this._collection.aggregate(pipeline, {
      ...options,
      session: this._session,
    } as any);
    return new MdbCursor<T>(cursor as any);
  }

  /**
   * Get distinct values for a key.
   * @param key - The field key.
   * @param filter - Optional filter.
   * @returns Array of distinct values.
   */
  async distinct<T = Document>(key: string, filter?: Filter<T>): Promise<any[]> {
    return await this._collection.distinct(key, filter as any, {
      session: this._session,
    } as any);
  }

  /**
   * Count documents matching a filter.
   * @param filter - Query filter.
   * @returns The count of matching documents.
   */
  async countDocuments(filter?: Document): Promise<number> {
    return await this._collection.countDocuments(filter as any, {
      session: this._session,
    } as any);
  }

  /**
   * Create an index on this collection.
   * @param keys - The index specification.
   * @param options - Index creation options.
   * @returns The name of the created index.
   */
  async createIndex(
    keys: IndexSpecification,
    options?: CreateIndexOptions
  ): Promise<string> {
    return await this._collection.createIndex(keys as any, {
      ...options,
      session: this._session,
    } as any);
  }

  /**
   * Drop an index by name.
   * @param name - The index name.
   */
  async dropIndex(name: string): Promise<void> {
    await this._collection.dropIndex(name, {
      session: this._session,
    } as any);
  }

  /**
   * List all indexes on this collection.
   * @returns Array of index information documents.
   */
  async listIndexes(): Promise<Document[]> {
    const indexes = await this._collection.listIndexes({ session: this._session } as any).toArray();
    return indexes as unknown as Document[];
  }

  /**
   * Watch for changes on this collection.
   * Returns a change stream for real-time monitoring.
   * @param pipeline - Optional aggregation pipeline stages for filtering.
   * @param options - Change stream options.
   */
  watch(
    pipeline?: Document[],
    options?: WatchOptions
  ): MdbChangeStream {
    const changeStream = this._collection.watch(pipeline as any, {
      ...options,
      session: this._session,
    } as any);
    return new MdbChangeStream(changeStream);
  }

  /**
   * Get a GridFS bucket for this collection.
   * GridFS is used for storing and retrieving large files.
   * @param options - GridFS bucket options.
   */
  gridfsBucket(options?: GridFSBucketOptions): MdbGridFSBucket {
    const bucket = new GridFSBucket(this._db.native, {
      bucketName: this.name,
      ...options,
    } as any);
    return new MdbGridFSBucket(bucket);
  }

  /**
   * Get collection statistics.
   * @returns Stats document.
   */
  async stats(): Promise<Document> {
    const result = await this._db.command({ collStats: this.name });
    return result as unknown as Document;
  }

  /**
   * Validate the collection's data and indexes.
   * @returns Validation result document.
   */
  async validate(): Promise<Document> {
    const result = await this._db.command(
      { validate: this.name }
    );
    return result as unknown as Document;
  }

  /**
   * Compact the collection to reclaim disk space.
   * Requires the compact privilege on the database.
   */
  async compact(): Promise<void> {
    await this._db.command(
      { compact: this.name }
    );
  }

  /**
   * Rebuild all indexes on the collection.
   */
  async reIndex(): Promise<void> {
    await this._db.command(
      { reIndex: this.name }
    );
  }

  /**
   * Rename the collection.
   * @param newName - The new collection name.
   * @param dropTarget - If true, drops an existing collection with the new name.
   */
  async rename(newName: string, dropTarget?: boolean): Promise<void> {
    await this._collection.rename(newName, { dropTarget } as any);
  }

  /**
   * Drop this collection.
   */
  async drop(): Promise<void> {
    await this._collection.drop();
  }
}
