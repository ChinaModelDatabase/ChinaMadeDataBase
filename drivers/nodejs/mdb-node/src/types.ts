import {
  Document,
  ObjectId,
  Timestamp,
  Filter as MongoFilter,
  UpdateFilter as MongoUpdateFilter,
  FindOptions as MongoFindOptions,
  InsertOneOptions as MongoInsertOneOptions,
  BulkWriteOptions as MongoBulkWriteOptions,
  UpdateOptions as MongoUpdateOptions,
  DeleteOptions as MongoDeleteOptions,
  ReplaceOptions as MongoReplaceOptions,
  AggregateOptions as MongoAggregateOptions,
  CreateCollectionOptions as MongoCreateCollectionOptions,
  CreateIndexesOptions as MongoCreateIndexesOptions,
  IndexSpecification as MongoIndexSpecification,
  ClientSessionOptions,
  TransactionOptions as MongoTransactionOptions,
  ChangeStreamDocument as MongoChangeStreamDocument,
  ChangeStreamOptions as MongoChangeStreamOptions,
  ResumeToken,
  GridFSBucketOptions as MongoGridFSBucketOptions,
  GridFSFile as MongoGridFSFile,
  MongoClientOptions,
  W as WriteConcernW,
  ReadConcernLevel,
} from "mongodb";

export type {
  Document,
  ObjectId,
  Timestamp,
  ResumeToken,
  ClientSessionOptions,
  MongoClientOptions,
  ReadConcernLevel,
} from "mongodb";

/** Extended document type for MDB-specific operations. */
export type MdbDocument = Document;

/** Filter type wrapping MongoDB's filter. */
export type Filter<T> = MongoFilter<T>;

/** Update filter type wrapping MongoDB's update filter. */
export type Update<T> = MongoUpdateFilter<T>;

/** Sort specification. */
export type Sort<T = Document> = string | { [key: string]: 1 | -1 | "asc" | "desc" | "ascending" | "descending" | { $meta: string } };

/** Projection specification. */
export type Projection = { [key: string]: 0 | 1 | boolean } | Document;

/** Index specification. */
export type IndexSpecification = MongoIndexSpecification;

/** Client connection options. */
export interface ClientOptions {
  appName?: string;
  authSource?: string;
  replicaSet?: string;
  directConnection?: boolean;
  connectTimeoutMS?: number;
  socketTimeoutMS?: number;
  serverSelectionTimeoutMS?: number;
  maxPoolSize?: number;
  minPoolSize?: number;
  maxIdleTimeMS?: number;
  readPreference?: string;
  compressors?: string[];
  retryWrites?: boolean;
  retryReads?: boolean;
  tls?: boolean;
  tlsCertificateKeyFile?: string;
  tlsCAFile?: string;
  tlsAllowInvalidCertificates?: boolean;
  /** Raw MongoDB client options passthrough. */
  mongoOptions?: MongoClientOptions;
}

/** Find options. */
export type FindOptions = Omit<MongoFindOptions, "session"> & {
  session?: any;
};

/** Insert one result. */
export interface InsertOneResult {
  insertedId: ObjectId;
  acknowledged: boolean;
}

/** Insert many result. */
export interface InsertManyResult {
  insertedIds: Record<number, ObjectId>;
  insertedCount: number;
  acknowledged: boolean;
}

/** Update result. */
export interface UpdateResult {
  matchedCount: number;
  modifiedCount: number;
  upsertedId: ObjectId | null;
  upsertedCount: number;
  acknowledged: boolean;
}

/** Delete result. */
export interface DeleteResult {
  deletedCount: number;
  acknowledged: boolean;
}

/** Session options. */
export type SessionOptions = ClientSessionOptions;

/** Transaction options. */
export type TransactionOptions = MongoTransactionOptions;

/** Create collection options. */
export type CreateCollectionOptions = MongoCreateCollectionOptions;

/** Create index options. */
export type CreateIndexOptions = MongoCreateIndexesOptions;

/** Aggregate options. */
export type AggregateOptions = MongoAggregateOptions;

/** Update options. */
export type UpdateOptions = MongoUpdateOptions;

/** Replace options. */
export type ReplaceOptions = MongoReplaceOptions;

/** Watch options for change streams. */
export type WatchOptions = MongoChangeStreamOptions;

/** Change stream document type. */
export type ChangeStreamDocument = MongoChangeStreamDocument;

/** GridFS bucket options. */
export type GridFSBucketOptions = MongoGridFSBucketOptions;

/** GridFS file metadata. */
export type GridFSFile = MongoGridFSFile;

/** Upload options for GridFS. */
export interface UploadOptions {
  contentType?: string;
  metadata?: Document;
  chunkSizeBytes?: number;
}

/** Role specification for user management. */
export interface Role {
  role: string;
  db?: string;
}

/** Profiler levels. */
export enum ProfilerLevel {
  Off = 0,
  SlowOnly = 1,
  All = 2,
}

/** Server heartbeat data. */
export interface HeartbeatData {
  serverAddress: string;
  durationMS: number;
  reply: Document;
}
