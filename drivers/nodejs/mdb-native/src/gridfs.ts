import { MdbClient } from './client';
import { MdbCollectionRef } from './database';
import { BsonDocument } from './bson';
import { ObjectId } from './objectid';
import { MdbCursor } from './cursor';
import { Readable } from 'stream';

const DEFAULT_CHUNK_SIZE = 255 * 1024; // 255 KiB

export interface GridFSFile {
  _id: ObjectId;
  filename: string;
  length: number;
  chunkSize: number;
  uploadDate: Date;
  metadata?: BsonDocument;
}

export interface GridFSUploadOptions {
  chunkSizeBytes?: number;
  metadata?: BsonDocument;
}

export class MdbGridFS {
  private client: MdbClient;
  private dbName: string;

  constructor(client: MdbClient, dbName: string) {
    this.client = client;
    this.dbName = dbName;
  }

  private coll(name: string): MdbCollectionRef {
    return this.client.db(this.dbName).collection(name);
  }

  private get chunks(): MdbCollectionRef { return this.coll('fs.chunks'); }
  private get files(): MdbCollectionRef { return this.coll('fs.files'); }

  async uploadFromBuffer(
    filename: string,
    data: Buffer,
    options: GridFSUploadOptions = {},
  ): Promise<ObjectId> {
    const chunkSize = options.chunkSizeBytes ?? DEFAULT_CHUNK_SIZE;
    const fileId = new ObjectId();
    const totalChunks = Math.ceil(data.length / chunkSize);

    const chunkDocs: BsonDocument[] = [];
    for (let i = 0; i < totalChunks; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, data.length);
      chunkDocs.push({
        files_id: fileId,
        n: i,
        data: data.subarray(start, end) as unknown as BsonDocument,
      });
    }

    if (chunkDocs.length > 0) {
      await this.chunks.insertMany(chunkDocs);
    }

    await this.files.insertOne({
      _id: fileId,
      filename,
      length: data.length,
      chunkSize,
      uploadDate: new Date(),
      metadata: options.metadata ?? null,
    });

    return fileId;
  }

  async uploadFromStream(
    filename: string,
    stream: Readable,
    options: GridFSUploadOptions = {},
  ): Promise<ObjectId> {
    const bufs: Buffer[] = [];
    for await (const chunk of stream) {
      bufs.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return this.uploadFromBuffer(filename, Buffer.concat(bufs), options);
  }

  async downloadToBuffer(fileId: ObjectId): Promise<Buffer> {
    const fileDoc = await this.files.findOne({ _id: fileId });
    if (!fileDoc) throw new Error(`File not found: ${fileId.toHexString()}`);

    const cursor = this.chunks.find({ filter: { files_id: fileId }, sort: { n: 1 } });
    const bufs: Buffer[] = [];
    await cursor.forEach((doc: BsonDocument) => {
      if (doc.data) bufs.push(doc.data as unknown as Buffer);
    });
    return Buffer.concat(bufs);
  }

  async downloadToBufferByName(filename: string): Promise<Buffer> {
    const fileDoc = await this.files.findOne({ filename });
    if (!fileDoc) throw new Error(`File not found: ${filename}`);
    return this.downloadToBuffer(fileDoc._id as ObjectId);
  }

  async find(filter: BsonDocument = {}): Promise<GridFSFile[]> {
    const cursor = this.files.find({ filter });
    return (await cursor.toArray()) as unknown as GridFSFile[];
  }

  async findOne(filter: BsonDocument): Promise<GridFSFile | null> {
    return (await this.files.findOne(filter)) as unknown as GridFSFile | null;
  }

  async delete(fileId: ObjectId): Promise<void> {
    await this.chunks.deleteMany({ files_id: fileId });
    await this.files.deleteMany({ _id: fileId });
  }

  async deleteByName(filename: string): Promise<void> {
    const fileDoc = await this.files.findOne({ filename });
    if (fileDoc) await this.delete(fileDoc._id as ObjectId);
  }

  async rename(fileId: ObjectId, newFilename: string): Promise<void> {
    await this.files.updateOne({ _id: fileId }, { $set: { filename: newFilename } });
  }

  async fileInfo(fileId: ObjectId): Promise<GridFSFile | null> {
    return this.findOne({ _id: fileId });
  }

  async createDownloadStream(fileId: ObjectId): Promise<Readable> {
    const fileDoc = await this.files.findOne({ _id: fileId });
    if (!fileDoc) throw new Error(`File not found: ${fileId.toHexString()}`);

    const cursor = this.chunks.find({
      filter: { files_id: fileId },
      sort: { n: 1 },
      batchSize: 4,
    });

    const readable = new Readable({
      async read() {
        const doc = await cursor.next();
        if (doc === null) {
          this.push(null);
        } else if (doc.data) {
          this.push(doc.data as unknown as Buffer);
        }
      },
    });

    return readable;
  }

  async createIndexes(): Promise<void> {
    await this.files.createIndex({ filename: 1 });
    await this.chunks.createIndex({ files_id: 1, n: 1 }, { unique: true } as unknown as BsonDocument);
  }
}
