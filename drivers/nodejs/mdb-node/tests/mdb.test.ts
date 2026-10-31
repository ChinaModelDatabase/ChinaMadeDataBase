import {
  MdbClient,
  MdbDatabase,
  MdbCollection,
  MdbCursor,
  MdbChangeStream,
  MdbSession,
  MdbTransaction,
  MdbGridFSBucket,
  MdbProfiler,
  WriteConcern,
  ReadConcern,
  ProfilerLevel,
} from "../src/index";
import {
  ascending,
  descending,
  compoundIndex,
  textIndex,
  geo2dIndex,
  geoSphereIndex,
  hashedIndex,
  buildIndexName,
  MdbIndexes,
  ASC,
  DESC,
} from "../src/index_helpers";
import { ObjectId } from "mongodb";

const mockId = new ObjectId();

const mockFindCursor = {
  sort: jest.fn().mockReturnThis(),
  limit: jest.fn().mockReturnThis(),
  skip: jest.fn().mockReturnThis(),
  batchSize: jest.fn().mockReturnThis(),
  project: jest.fn().mockReturnThis(),
  next: jest.fn().mockResolvedValue(null),
  toArray: jest.fn().mockResolvedValue([]),
  explain: jest.fn().mockResolvedValue({}),
  close: jest.fn().mockResolvedValue(undefined),
  closed: false,
  remainingBatchSize: jest.fn().mockReturnValue(0),
};

const mockCollection = {
  find: jest.fn().mockReturnValue({ ...mockFindCursor }),
  findOne: jest.fn().mockResolvedValue(null),
  insertOne: jest.fn().mockResolvedValue({ insertedId: mockId, acknowledged: true }),
  insertMany: jest.fn().mockResolvedValue({ insertedIds: { 0: mockId }, insertedCount: 1, acknowledged: true }),
  updateOne: jest.fn().mockResolvedValue({ matchedCount: 1, modifiedCount: 1, upsertedId: null, upsertedCount: 0, acknowledged: true }),
  updateMany: jest.fn().mockResolvedValue({ matchedCount: 1, modifiedCount: 1, upsertedId: null, upsertedCount: 0, acknowledged: true }),
  deleteOne: jest.fn().mockResolvedValue({ deletedCount: 1, acknowledged: true }),
  deleteMany: jest.fn().mockResolvedValue({ deletedCount: 1, acknowledged: true }),
  replaceOne: jest.fn().mockResolvedValue({ matchedCount: 1, modifiedCount: 1, upsertedId: null, upsertedCount: 0, acknowledged: true }),
  aggregate: jest.fn().mockReturnValue({
    toArray: jest.fn().mockResolvedValue([]),
    next: jest.fn().mockResolvedValue(null),
    close: jest.fn().mockResolvedValue(undefined),
    closed: false,
    limit: jest.fn().mockReturnThis(),
    skip: jest.fn().mockReturnThis(),
    batchSize: jest.fn().mockReturnThis(),
  }),
  distinct: jest.fn().mockResolvedValue([]),
  countDocuments: jest.fn().mockResolvedValue(0),
  createIndex: jest.fn().mockResolvedValue("index_name"),
  dropIndex: jest.fn().mockResolvedValue(undefined),
  listIndexes: jest.fn().mockReturnValue({ toArray: jest.fn().mockResolvedValue([]) }),
  watch: jest.fn().mockReturnValue({
    next: jest.fn().mockResolvedValue(null),
    tryNext: jest.fn().mockResolvedValue(null),
    close: jest.fn().mockResolvedValue(undefined),
    resumeAfter: jest.fn().mockReturnThis(),
    startAfter: jest.fn().mockReturnThis(),
    startAtOperationTime: jest.fn().mockReturnThis(),
    maxAwaitTimeMS: jest.fn().mockReturnThis(),
    isAlive: true,
    resumeToken: undefined,
  }),
  stats: jest.fn().mockResolvedValue({}),
  rename: jest.fn().mockResolvedValue(undefined),
  drop: jest.fn().mockResolvedValue(undefined),
  collectionName: "testcoll",
};

const mockAdmin = {
  listDatabases: jest.fn().mockResolvedValue({ databases: [] }),
  command: jest.fn().mockResolvedValue({}),
};

const mockDb = {
  collection: jest.fn().mockReturnValue(mockCollection),
  command: jest.fn().mockResolvedValue({}),
  admin: jest.fn().mockReturnValue(mockAdmin),
  listCollections: jest.fn().mockReturnValue({
    toArray: jest.fn().mockResolvedValue([]),
  }),
  createCollection: jest.fn().mockResolvedValue(undefined),
  dropCollection: jest.fn().mockResolvedValue(undefined),
  databaseName: "testdb",
};

const mockSession = {
  id: "session-id",
  inTransaction: jest.fn().mockReturnValue(false),
  transaction: { state: "NO_TRANSACTION" },
  startTransaction: jest.fn().mockResolvedValue(undefined),
  commitTransaction: jest.fn().mockResolvedValue(undefined),
  abortTransaction: jest.fn().mockResolvedValue(undefined),
  endSession: jest.fn().mockResolvedValue(undefined),
};

const mockUploadStream = {
  id: mockId,
  end: jest.fn(),
  on: jest.fn((event: string, cb: any) => {
    if (event === "finish") {
      setTimeout(() => cb(), 0);
    }
  }),
};

const mockDownloadStream = {
  on: jest.fn((event: string, cb: any) => {
    if (event === "data") {
      setTimeout(() => cb(Buffer.from("test")), 0);
    }
    if (event === "end") {
      setTimeout(() => cb(), 10);
    }
  }),
};

jest.mock("mongodb", () => {
  const actual = jest.requireActual("mongodb");
  return {
    ...actual,
    MongoClient: jest.fn().mockImplementation(() => ({
      connect: jest.fn().mockResolvedValue(undefined),
      close: jest.fn().mockResolvedValue(undefined),
      isConnected: jest.fn().mockReturnValue(true),
      db: jest.fn().mockReturnValue(mockDb),
      admin: jest.fn().mockReturnValue(mockAdmin),
      startSession: jest.fn().mockReturnValue(mockSession),
    })),
    GridFSBucket: jest.fn().mockImplementation(() => ({
      openUploadStream: jest.fn().mockReturnValue(mockUploadStream),
      openDownloadStream: jest.fn().mockReturnValue(mockDownloadStream),
      find: jest.fn().mockReturnValue({
        toArray: jest.fn().mockResolvedValue([]),
      }),
      delete: jest.fn().mockResolvedValue(undefined),
      rename: jest.fn().mockResolvedValue(undefined),
    })),
    ObjectId: actual.ObjectId,
  };
});

describe("MdbClient", () => {
  let client: MdbClient;

  beforeEach(() => {
    client = MdbClient.create("localhost", 27017);
  });

  afterEach(async () => {
    await client.close();
  });

  test("should create client from host and port", () => {
    expect(client).toBeDefined();
    expect(client).toBeInstanceOf(MdbClient);
  });

  test("should create client from URI", () => {
    const uriClient = MdbClient.fromURI("mongodb://localhost:27017");
    expect(uriClient).toBeDefined();
  });

  test("should create client with authentication", () => {
    const authClient = MdbClient.createWithAuth("localhost", 27017, "admin", "secret");
    expect(authClient).toBeDefined();
  });

  test("should connect to server", async () => {
    await expect(client.connect()).resolves.toBeUndefined();
  });

  test("should get database reference", () => {
    const db = client.database("testdb");
    expect(db).toBeDefined();
    expect(db).toBeInstanceOf(MdbDatabase);
  });

  test("should list databases", async () => {
    await client.connect();
    const databases = await client.listDatabases();
    expect(Array.isArray(databases)).toBe(true);
  });

  test("should get server status", async () => {
    await client.connect();
    const status = await client.serverStatus();
    expect(status).toBeDefined();
  });

  test("should check connection status", async () => {
    await client.connect();
    expect(typeof client.isConnected()).toBe("boolean");
  });

  test("should close connection", async () => {
    await client.connect();
    await client.close();
  });

  test("should start session", () => {
    const session = client.startSession();
    expect(session).toBeDefined();
    expect(session).toBeInstanceOf(MdbSession);
  });

  test("should start transaction", async () => {
    const txn = await client.startTransaction();
    expect(txn).toBeDefined();
    expect(txn).toBeInstanceOf(MdbTransaction);
    await txn.commit();
  });

  test("should support async dispose", async () => {
    const disposeClient = MdbClient.create("localhost", 27017);
    await disposeClient.connect();
    await disposeClient[Symbol.asyncDispose]();
  });

  test("should return native MongoClient", () => {
    expect(client.native).toBeDefined();
  });
});

describe("MdbDatabase", () => {
  let client: MdbClient;
  let db: MdbDatabase;

  beforeEach(async () => {
    client = MdbClient.create("localhost", 27017);
    await client.connect();
    db = client.database("testdb");
  });

  afterEach(async () => {
    await client.close();
  });

  test("should get database name", () => {
    expect(db.name).toBe("testdb");
  });

  test("should get native Db instance", () => {
    expect(db.native).toBeDefined();
  });

  test("should get collection reference", () => {
    const coll = db.collection("testcoll");
    expect(coll).toBeDefined();
    expect(coll).toBeInstanceOf(MdbCollection);
  });

  test("should list collections", async () => {
    const collections = await db.listCollections();
    expect(Array.isArray(collections)).toBe(true);
  });

  test("should create collection", async () => {
    await expect(db.createCollection("newcoll")).resolves.toBeUndefined();
  });

  test("should drop collection", async () => {
    await expect(db.dropCollection("oldcoll")).resolves.toBeUndefined();
  });

  test("should execute command", async () => {
    const result = await db.command({ ping: 1 });
    expect(result).toBeDefined();
  });

  test("should get database stats", async () => {
    const stats = await db.stats();
    expect(stats).toBeDefined();
  });

  test("should get profiler", () => {
    const profiler = db.profiler();
    expect(profiler).toBeDefined();
    expect(profiler).toBeInstanceOf(MdbProfiler);
  });

  test("should create user", async () => {
    await expect(
      db.createUser("testuser", "password123")
    ).resolves.toBeUndefined();
  });

  test("should drop user", async () => {
    await expect(db.dropUser("testuser")).resolves.toBeUndefined();
  });

  test("should get users", async () => {
    const users = await db.getUsers();
    expect(Array.isArray(users)).toBe(true);
  });

  test("should get client reference", () => {
    expect(db.client).toBe(client);
  });

  test("should create database with session", () => {
    const session = client.startSession();
    const dbWithSession = db.withSession(session.native);
    expect(dbWithSession).toBeDefined();
    expect(dbWithSession).toBeInstanceOf(MdbDatabase);
  });
});

describe("MdbCollection", () => {
  let client: MdbClient;
  let db: MdbDatabase;
  let coll: MdbCollection;

  beforeEach(async () => {
    client = MdbClient.create("localhost", 27017);
    await client.connect();
    db = client.database("testdb");
    coll = db.collection("testcoll");
  });

  afterEach(async () => {
    await client.close();
  });

  test("should get collection name", () => {
    expect(coll.name).toBe("testcoll");
  });

  test("should get native Collection", () => {
    expect(coll.native).toBeDefined();
  });

  test("should get parent database", () => {
    expect(coll.database).toBe(db);
  });

  test("should find documents", () => {
    const cursor = coll.find({ name: "test" });
    expect(cursor).toBeDefined();
    expect(cursor).toBeInstanceOf(MdbCursor);
  });

  test("should find one document", async () => {
    const doc = await coll.findOne({ name: "test" });
    expect(doc).toBeNull();
  });

  test("should insert one document", async () => {
    const result = await coll.insertOne({ name: "Alice", age: 30 });
    expect(result.insertedId).toBeDefined();
    expect(result.acknowledged).toBe(true);
  });

  test("should insert many documents", async () => {
    const result = await coll.insertMany([
      { name: "Bob" },
      { name: "Charlie" },
    ]);
    expect(result.insertedCount).toBeDefined();
    expect(result.acknowledged).toBe(true);
  });

  test("should update one document", async () => {
    const result = await coll.updateOne(
      { name: "Alice" },
      { $set: { age: 31 } }
    );
    expect(result.matchedCount).toBe(1);
    expect(result.modifiedCount).toBe(1);
  });

  test("should update many documents", async () => {
    const result = await coll.updateMany(
      { age: { $lt: 18 } },
      { $set: { minor: true } }
    );
    expect(result.matchedCount).toBeDefined();
  });

  test("should delete one document", async () => {
    const result = await coll.deleteOne({ name: "Alice" });
    expect(result.deletedCount).toBe(1);
  });

  test("should delete many documents", async () => {
    const result = await coll.deleteMany({ age: { $lt: 18 } });
    expect(result.deletedCount).toBeDefined();
  });

  test("should replace one document", async () => {
    const result = await coll.replaceOne(
      { name: "Alice" },
      { name: "Alice Updated", age: 32 }
    );
    expect(result.matchedCount).toBe(1);
  });

  test("should aggregate", () => {
    const cursor = coll.aggregate([{ $group: { _id: "$status" } }]);
    expect(cursor).toBeDefined();
    expect(cursor).toBeInstanceOf(MdbCursor);
  });

  test("should get distinct values", async () => {
    const values = await coll.distinct("name");
    expect(Array.isArray(values)).toBe(true);
  });

  test("should count documents", async () => {
    const count = await coll.countDocuments({ name: "test" });
    expect(count).toBe(0);
  });

  test("should create index", async () => {
    const indexName = await coll.createIndex({ name: 1 });
    expect(indexName).toBe("index_name");
  });

  test("should drop index", async () => {
    await expect(coll.dropIndex("index_name")).resolves.toBeUndefined();
  });

  test("should list indexes", async () => {
    const indexes = await coll.listIndexes();
    expect(Array.isArray(indexes)).toBe(true);
  });

  test("should watch for changes", () => {
    const changeStream = coll.watch();
    expect(changeStream).toBeDefined();
    expect(changeStream).toBeInstanceOf(MdbChangeStream);
  });

  test("should get GridFS bucket", () => {
    const bucket = coll.gridfsBucket();
    expect(bucket).toBeDefined();
    expect(bucket).toBeInstanceOf(MdbGridFSBucket);
  });

  test("should get stats", async () => {
    const stats = await coll.stats();
    expect(stats).toBeDefined();
  });

  test("should validate collection", async () => {
    const result = await coll.validate();
    expect(result).toBeDefined();
  });

  test("should compact collection", async () => {
    await expect(coll.compact()).resolves.toBeUndefined();
  });

  test("should reindex collection", async () => {
    await expect(coll.reIndex()).resolves.toBeUndefined();
  });

  test("should rename collection", async () => {
    await expect(coll.rename("newname")).resolves.toBeUndefined();
  });

  test("should drop collection", async () => {
    await expect(coll.drop()).resolves.toBeUndefined();
  });
});

describe("MdbCursor", () => {
  let client: MdbClient;
  let coll: MdbCollection;

  beforeEach(async () => {
    client = MdbClient.create("localhost", 27017);
    await client.connect();
    coll = client.database("testdb").collection("testcoll");
  });

  afterEach(async () => {
    await client.close();
  });

  test("should chain sort", () => {
    const cursor = coll.find();
    const sorted = cursor.sort({ name: 1 });
    expect(sorted).toBeInstanceOf(MdbCursor);
  });

  test("should chain limit", () => {
    const cursor = coll.find();
    const limited = cursor.limit(10);
    expect(limited).toBeInstanceOf(MdbCursor);
  });

  test("should chain skip", () => {
    const cursor = coll.find();
    const skipped = cursor.skip(5);
    expect(skipped).toBeInstanceOf(MdbCursor);
  });

  test("should chain batchSize", () => {
    const cursor = coll.find();
    const batched = cursor.batchSize(50);
    expect(batched).toBeInstanceOf(MdbCursor);
  });

  test("should chain projection", () => {
    const cursor = coll.find();
    const projected = cursor.projection({ name: 1, _id: 0 });
    expect(projected).toBeInstanceOf(MdbCursor);
  });

  test("should get next document", async () => {
    const cursor = coll.find();
    const doc = await cursor.next();
    expect(doc).toBeNull();
  });

  test("should convert to array", async () => {
    const cursor = coll.find();
    const arr = await cursor.toArray();
    expect(Array.isArray(arr)).toBe(true);
  });

  test("should get explanation", async () => {
    const cursor = coll.find();
    const explanation = await cursor.explain();
    expect(explanation).toBeDefined();
  });

  test("should close cursor", async () => {
    const cursor = coll.find();
    await expect(cursor.close()).resolves.toBeUndefined();
  });

  test("should check if closed", () => {
    const cursor = coll.find();
    expect(typeof cursor.closed).toBe("boolean");
  });

  test("should implement async iterator", () => {
    const cursor = coll.find();
    expect(typeof cursor[Symbol.asyncIterator]).toBe("function");
  });
});

describe("MdbChangeStream", () => {
  let client: MdbClient;
  let coll: MdbCollection;

  beforeEach(async () => {
    client = MdbClient.create("localhost", 27017);
    await client.connect();
    coll = client.database("testdb").collection("testcoll");
  });

  afterEach(async () => {
    await client.close();
  });

  test("should get next change", async () => {
    const stream = coll.watch();
    const change = await stream.next();
    expect(change).toBeNull();
  });

  test("should try next change", async () => {
    const stream = coll.watch();
    const change = await stream.tryNext();
    expect(change).toBeNull();
  });

  test("should close stream", async () => {
    const stream = coll.watch();
    await expect(stream.close()).resolves.toBeUndefined();
  });

  test("should check if alive", () => {
    const stream = coll.watch();
    expect(stream.isAlive).toBe(true);
  });

  test("should get resume token", () => {
    const stream = coll.watch();
    expect(stream.resumeToken).toBeUndefined();
  });

  test("should implement async iterator", () => {
    const stream = coll.watch();
    expect(typeof stream[Symbol.asyncIterator]).toBe("function");
  });
});

describe("MdbSession", () => {
  let client: MdbClient;

  beforeEach(async () => {
    client = MdbClient.create("localhost", 27017);
    await client.connect();
  });

  afterEach(async () => {
    await client.close();
  });

  test("should start and commit transaction", async () => {
    const session = client.startSession();
    await session.startTransaction();
    expect(session.inTransaction).toBe(false);
    await session.commitTransaction();
    await session.endSession();
  });

  test("should abort transaction", async () => {
    const session = client.startSession();
    await session.startTransaction();
    await session.abortTransaction();
    await session.endSession();
  });

  test("should get session ID", () => {
    const session = client.startSession();
    expect(session.id).toBeDefined();
  });

  test("should get native session", () => {
    const session = client.startSession();
    expect(session.native).toBeDefined();
  });

  test("should get client reference", () => {
    const session = client.startSession();
    expect(session.client).toBe(client);
  });
});

describe("MdbTransaction", () => {
  let client: MdbClient;

  beforeEach(async () => {
    client = MdbClient.create("localhost", 27017);
    await client.connect();
  });

  afterEach(async () => {
    await client.close();
  });

  test("should commit transaction", async () => {
    const txn = await client.startTransaction();
    expect(txn).toBeDefined();
    await txn.commit();
  });

  test("should abort transaction", async () => {
    const txn = await client.startTransaction();
    await txn.abort();
  });

  test("should get database from transaction", async () => {
    const txn = await client.startTransaction();
    const db = txn.database("testdb");
    expect(db).toBeDefined();
    await txn.commit();
  });

  test("should check if active", async () => {
    const txn = await client.startTransaction();
    expect(typeof txn.isActive).toBe("boolean");
    await txn.commit();
  });
});

describe("MdbGridFSBucket", () => {
  let client: MdbClient;
  let bucket: MdbGridFSBucket;

  beforeEach(async () => {
    client = MdbClient.create("localhost", 27017);
    await client.connect();
    bucket = client
      .database("testdb")
      .collection("fs")
      .gridfsBucket();
  });

  afterEach(async () => {
    await client.close();
  });

  test("should upload from buffer", async () => {
    const data = Buffer.from("test file content");
    const fileId = await bucket.uploadFromBytes("test.txt", data);
    expect(fileId).toBeDefined();
  });

  test("should find files", async () => {
    const files = await bucket.find();
    expect(Array.isArray(files)).toBe(true);
  });

  test("should get native bucket", () => {
    expect(bucket.native).toBeDefined();
  });
});

describe("MdbProfiler", () => {
  let client: MdbClient;
  let profiler: MdbProfiler;

  beforeEach(async () => {
    client = MdbClient.create("localhost", 27017);
    await client.connect();
    profiler = client.database("testdb").profiler();
  });

  afterEach(async () => {
    await client.close();
  });

  test("should enable profiler", async () => {
    await expect(profiler.enable(100)).resolves.toBeUndefined();
  });

  test("should disable profiler", async () => {
    await expect(profiler.disable()).resolves.toBeUndefined();
  });

  test("should get slow queries", async () => {
    const queries = await profiler.slowQueries();
    expect(Array.isArray(queries)).toBe(true);
  });

  test("should get stats", async () => {
    const stats = await profiler.stats();
    expect(stats).toBeDefined();
  });

  test("should get profiler level", async () => {
    mockDb.command.mockResolvedValueOnce({ was: 1, slowms: 100 });
    const level = await profiler.getLevel();
    expect(level).toBe(1);
  });

  test("should check if enabled", async () => {
    const enabled = await profiler.isEnabled();
    expect(typeof enabled).toBe("boolean");
  });
});

describe("WriteConcern", () => {
  test("should create default write concern", () => {
    const wc = new WriteConcern(1);
    expect(wc.w).toBe(1);
  });

  test("should create majority write concern", () => {
    const wc = WriteConcern.MAJORITY;
    expect(wc.w).toBe("majority");
  });

  test("should create unacknowledged write concern", () => {
    const wc = WriteConcern.UNACKNOWLEDGED;
    expect(wc.isUnacknowledged).toBe(true);
  });

  test("should convert to document", () => {
    const wc = new WriteConcern("majority", 5000, true);
    const doc = wc.toDocument();
    expect(doc.w).toBe("majority");
    expect(doc.wtimeoutMS).toBe(5000);
    expect(doc.journal).toBe(true);
  });
});

describe("ReadConcern", () => {
  test("should create local read concern", () => {
    const rc = ReadConcern.LOCAL;
    expect(rc.level).toBe("local");
  });

  test("should create majority read concern", () => {
    const rc = ReadConcern.MAJORITY;
    expect(rc.level).toBe("majority");
  });

  test("should create linearizable read concern", () => {
    const rc = ReadConcern.LINEARIZABLE;
    expect(rc.level).toBe("linearizable");
  });

  test("should create snapshot read concern", () => {
    const rc = ReadConcern.SNAPSHOT;
    expect(rc.level).toBe("snapshot");
  });

  test("should convert to document", () => {
    const rc = new ReadConcern("majority");
    const doc = rc.toDocument();
    expect(doc.level).toBe("majority");
  });
});

describe("Index Helpers", () => {
  test("should create ascending index", () => {
    const spec = ascending("name");
    expect(spec).toEqual({ name: 1 });
  });

  test("should create descending index", () => {
    const spec = descending("createdAt");
    expect(spec).toEqual({ createdAt: -1 });
  });

  test("should create compound index", () => {
    const spec = compoundIndex(["name", 1], ["age", -1]);
    expect(spec).toEqual({ name: 1, age: -1 });
  });

  test("should create text index", () => {
    const spec = textIndex("description");
    expect(spec).toEqual({ description: "text" });
  });

  test("should create geo2d index", () => {
    const spec = geo2dIndex("location");
    expect(spec).toEqual({ location: "2d" });
  });

  test("should create 2dsphere index", () => {
    const spec = geoSphereIndex("location");
    expect(spec).toEqual({ location: "2dsphere" });
  });

  test("should create hashed index", () => {
    const spec = hashedIndex("email");
    expect(spec).toEqual({ email: "hashed" });
  });

  test("should build index name", () => {
    const name = buildIndexName({ name: 1, age: -1 });
    expect(name).toBe("name_1_age_-1");
  });

  test("should have MdbIndexes helpers", () => {
    expect(MdbIndexes.id()).toEqual({ _id: 1 });
    expect(MdbIndexes.createdAt()).toEqual({ createdAt: -1, _id: -1 });
    expect(MdbIndexes.ttl("expiresAt", 3600)).toEqual({ expireAfterSeconds: 3600 });
  });

  test("should create TTL index options", () => {
    const opts = require("../src/index_helpers").ttlIndex("expiresAt", 3600);
    expect(opts.expireAfterSeconds).toBe(3600);
  });

  test("should create unique index options", () => {
    const opts = require("../src/index_helpers").uniqueIndex("email");
    expect(opts.unique).toBe(true);
  });

  test("should create sparse index options", () => {
    const opts = require("../src/index_helpers").sparseIndex("optionalField");
    expect(opts.sparse).toBe(true);
  });

  test("should create partial filter options", () => {
    const opts = require("../src/index_helpers").partialFilter({ status: "active" });
    expect(opts.partialFilterExpression).toEqual({ status: "active" });
  });
});

describe("Types", () => {
  test("should export ProfilerLevel enum", () => {
    expect(ProfilerLevel.Off).toBe(0);
    expect(ProfilerLevel.SlowOnly).toBe(1);
    expect(ProfilerLevel.All).toBe(2);
  });
});
