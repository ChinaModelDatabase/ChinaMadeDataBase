import { MdbClient } from './client';
import { BsonDocument } from './bson';
import { MdbCursor } from './cursor';

export class MdbDatabase {
  private client: MdbClient;
  private _name: string;

  constructor(client: MdbClient, name: string) {
    this.client = client;
    this._name = name;
  }

  get name(): string {
    return this._name;
  }

  collection(name: string): MdbCollectionRef {
    return new MdbCollectionRef(this.client, this._name, name);
  }

  async command(cmd: BsonDocument): Promise<BsonDocument> {
    return this.client.command(this._name, cmd);
  }

  async listCollections(): Promise<BsonDocument[]> {
    const result = await this.command({ listCollections: 1, filter: {} });
    const cursor = result.cursor as any;
    return cursor?.firstBatch ?? [];
  }

  async dropDatabase(): Promise<BsonDocument> {
    return this.command({ dropDatabase: 1 });
  }

  async stats(): Promise<BsonDocument> {
    return this.command({ dbStats: 1 });
  }
}

export class MdbCollectionRef {
  readonly client: MdbClient;
  readonly dbName: string;
  readonly name: string;

  constructor(client: MdbClient, dbName: string, collName: string) {
    this.client = client;
    this.dbName = dbName;
    this.name = collName;
  }

  private _makeColl(): any {
    const mod = require('./collection');
    return new mod.MdbCollection(this.client, this.dbName, this.name);
  }

  async sendCommand(cmd: BsonDocument): Promise<BsonDocument> {
    return this._makeColl().sendCommand(cmd);
  }

  async insertOne(doc: BsonDocument): Promise<any> {
    return this._makeColl().insertOne(doc);
  }

  async insertMany(docs: BsonDocument[]): Promise<any> {
    return this._makeColl().insertMany(docs);
  }

  async findOne(filter: BsonDocument = {}, projection?: BsonDocument): Promise<BsonDocument | null> {
    return this._makeColl().findOne(filter, projection);
  }

  find(opts: any = {}): MdbCursor {
    return this._makeColl().find(opts);
  }

  async updateOne(filter: BsonDocument, update: BsonDocument, options?: any): Promise<any> {
    return this._makeColl().updateOne(filter, update, options);
  }

  async updateMany(filter: BsonDocument, update: BsonDocument, options?: any): Promise<any> {
    return this._makeColl().updateMany(filter, update, options);
  }

  async deleteOne(filter: BsonDocument): Promise<any> {
    return this._makeColl().deleteOne(filter);
  }

  async deleteMany(filter: BsonDocument): Promise<any> {
    return this._makeColl().deleteMany(filter);
  }

  async countDocuments(filter: BsonDocument = {}): Promise<number> {
    return this._makeColl().countDocuments(filter);
  }

  async distinct(field: string, filter: BsonDocument = {}): Promise<any[]> {
    return this._makeColl().distinct(field, filter);
  }

  async aggregate(pipeline: BsonDocument[]): Promise<MdbCursor> {
    return this._makeColl().aggregate(pipeline);
  }

  async drop(): Promise<boolean> {
    return this._makeColl().drop();
  }

  async createIndex(indexSpec: BsonDocument, options?: BsonDocument): Promise<string> {
    return this._makeColl().createIndex(indexSpec, options);
  }

  async listIndexes(): Promise<BsonDocument[]> {
    return this._makeColl().listIndexes();
  }
}
