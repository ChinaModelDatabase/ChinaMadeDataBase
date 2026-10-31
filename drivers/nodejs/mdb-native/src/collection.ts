import { MdbClient } from './client';
import { BsonDocument } from './bson';
import { MdbCursor } from './cursor';
import { nextReqId, OpMsgMessage } from './wire_protocol';

export interface InsertOneResult {
  insertedId: any;
}

export interface InsertManyResult {
  insertedIds: any[];
  insertedCount: number;
}

export interface UpdateResult {
  matchedCount: number;
  modifiedCount: number;
  upsertedId: any | null;
}

export interface DeleteResult {
  deletedCount: number;
}

export interface FindOptions {
  filter?: BsonDocument;
  projection?: BsonDocument;
  sort?: BsonDocument;
  limit?: number;
  skip?: number;
  batchSize?: number;
}

export interface UpdateOptions {
  upsert?: boolean;
  multi?: boolean;
}

export class MdbCollection {
  private client: MdbClient;
  private _dbName: string;
  private _name: string;

  constructor(client: MdbClient, dbName: string, collName: string) {
    this.client = client;
    this._dbName = dbName;
    this._name = collName;
  }

  get dbName(): string {
    return this._dbName;
  }

  get name(): string {
    return this._name;
  }

  async sendCommand(cmd: BsonDocument): Promise<BsonDocument> {
    const msg: OpMsgMessage = {
      requestID: nextReqId(),
      responseTo: 0,
      flags: { checksumPresent: false, moreToCome: false, exhaustAllowed: false },
      sections: [{ kind: 0, document: { ...cmd, $db: this._dbName } }],
    };
    const reply = await this.client._getConnection().send(msg);
    for (const section of reply.sections) {
      if (section.kind === 0) return section.document;
    }
    throw new Error('No body in response');
  }

  async insertOne(doc: BsonDocument): Promise<InsertOneResult> {
    const result = await this.sendCommand({
      insert: this._name,
      documents: [doc],
    });
    const writeErrors = result.writeErrors as any;
    if (writeErrors && writeErrors.length > 0) {
      throw new Error(`Insert error: ${writeErrors[0].errmsg}`);
    }
    return { insertedId: doc._id ?? null };
  }

  async insertMany(docs: BsonDocument[]): Promise<InsertManyResult> {
    const result = await this.sendCommand({
      insert: this._name,
      documents: docs,
    });
    const writeErrors = result.writeErrors as any;
    if (writeErrors && writeErrors.length > 0) {
      throw new Error(`Insert error: ${writeErrors[0].errmsg}`);
    }
    const ids = docs.map((d) => d._id ?? null);
    const n = result.n as any;
    return { insertedIds: ids, insertedCount: typeof n === 'number' ? n : docs.length };
  }

  find(opts: FindOptions = {}): MdbCursor {
    const filter = opts.filter ?? {};
    const cmd: BsonDocument = {
      find: this._name,
      filter,
      $db: this._dbName,
    };
    if (opts.projection) cmd.projection = opts.projection;
    if (opts.sort) cmd.sort = opts.sort;
    if (opts.limit !== undefined) cmd.limit = opts.limit;
    if (opts.skip !== undefined) cmd.skip = opts.skip;
    if (opts.batchSize !== undefined) cmd.batchSize = opts.batchSize;
    return new MdbCursor(this.client, this._dbName, cmd);
  }

  async findOne(filter: BsonDocument = {}, projection?: BsonDocument): Promise<BsonDocument | null> {
    const cmd: BsonDocument = {
      find: this._name,
      filter,
      limit: 1,
      $db: this._dbName,
    };
    if (projection) cmd.projection = projection;
    const msg: OpMsgMessage = {
      requestID: nextReqId(),
      responseTo: 0,
      flags: { checksumPresent: false, moreToCome: false, exhaustAllowed: false },
      sections: [{ kind: 0, document: cmd }],
    };
    const reply = await this.client._getConnection().send(msg);
    for (const section of reply.sections) {
      if (section.kind === 0) {
        const cursor = section.document.cursor as any;
        if (cursor && cursor.firstBatch && cursor.firstBatch.length > 0) {
          return cursor.firstBatch[0];
        }
      }
    }
    return null;
  }

  async updateOne(filter: BsonDocument, update: BsonDocument, options: UpdateOptions = {}): Promise<UpdateResult> {
    const cmd: BsonDocument = {
      update: this._name,
      updates: [{ q: filter, u: update, upsert: options.upsert ?? false }],
    };
    const result = await this.sendCommand(cmd);
    const n = result.n as any;
    const nMod = result.nModified as any;
    const upserted = result.upserted as any;
    return {
      matchedCount: typeof n === 'number' ? n : 0,
      modifiedCount: typeof nMod === 'number' ? nMod : 0,
      upsertedId: upserted?.[0]?._id ?? null,
    };
  }

  async updateMany(filter: BsonDocument, update: BsonDocument, options: UpdateOptions = {}): Promise<UpdateResult> {
    const cmd: BsonDocument = {
      update: this._name,
      updates: [{ q: filter, u: update, multi: true, upsert: options.upsert ?? false }],
    };
    const result = await this.sendCommand(cmd);
    const n = result.n as any;
    const nMod = result.nModified as any;
    const upserted = result.upserted as any;
    return {
      matchedCount: typeof n === 'number' ? n : 0,
      modifiedCount: typeof nMod === 'number' ? nMod : 0,
      upsertedId: upserted?.[0]?._id ?? null,
    };
  }

  async replaceOne(filter: BsonDocument, replacement: BsonDocument, options: UpdateOptions = {}): Promise<UpdateResult> {
    return this.updateOne(filter, replacement, options);
  }

  async deleteOne(filter: BsonDocument): Promise<DeleteResult> {
    const result = await this.sendCommand({
      delete: this._name,
      deletes: [{ q: filter, limit: 1 }],
    });
    const n = result.n as any;
    return { deletedCount: typeof n === 'number' ? n : 0 };
  }

  async deleteMany(filter: BsonDocument): Promise<DeleteResult> {
    const result = await this.sendCommand({
      delete: this._name,
      deletes: [{ q: filter, limit: 0 }],
    });
    const n = result.n as any;
    return { deletedCount: typeof n === 'number' ? n : 0 };
  }

  async countDocuments(filter: BsonDocument = {}): Promise<number> {
    const result = await this.sendCommand({ count: this._name, query: filter });
    const n = result.n as any;
    return typeof n === 'number' ? n : 0;
  }

  async distinct(field: string, filter: BsonDocument = {}): Promise<any[]> {
    const result = await this.sendCommand({ distinct: this._name, key: field, query: filter });
    return (result.values as any) ?? [];
  }

  async aggregate(pipeline: BsonDocument[]): Promise<MdbCursor> {
    const cmd: BsonDocument = {
      aggregate: this._name,
      pipeline,
      cursor: {},
      $db: this._dbName,
    };
    return new MdbCursor(this.client, this._dbName, cmd);
  }

  async drop(): Promise<boolean> {
    const result = await this.sendCommand({ drop: this._name });
    return result.ns !== undefined;
  }

  async createIndex(indexSpec: BsonDocument, options?: BsonDocument): Promise<string> {
    const cmd: BsonDocument = {
      createIndexes: this._name,
      indexes: [{ key: indexSpec, name: Object.keys(indexSpec).join('_'), ...options }],
    };
    const result = await this.sendCommand(cmd);
    return result.ok === 1 ? 'ok' : 'error';
  }

  async listIndexes(): Promise<BsonDocument[]> {
    const result = await this.sendCommand({ listIndexes: this._name });
    const cursor = result.cursor as any;
    return cursor?.firstBatch ?? [];
  }
}
