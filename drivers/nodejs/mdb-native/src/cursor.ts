import { MdbClient } from './client';
import { BsonDocument } from './bson';
import { HEADER_SIZE, OpMsgMessage, parseOpMsg, nextReqId, OP_MSG } from './wire_protocol';

export class MdbCursor implements AsyncIterable<BsonDocument> {
  private client: MdbClient;
  private dbName: string;
  private cmd: BsonDocument;
  private cursorId: bigint = 0n;
  private buffer: BsonDocument[] = [];
  private exhausted = false;
  private started = false;

  constructor(client: MdbClient, dbName: string, cmd: BsonDocument) {
    this.client = client;
    this.dbName = dbName;
    this.cmd = { ...cmd };
  }

  private async sendCommand(doc: BsonDocument): Promise<BsonDocument> {
    const conn = this.client._getConnection();
    const msg: OpMsgMessage = {
      requestID: nextReqId(),
      responseTo: 0,
      flags: { checksumPresent: false, moreToCome: false, exhaustAllowed: false },
      sections: [{ kind: 0, document: doc }],
    };
    const reply = await conn.send(msg);
    for (const section of reply.sections) {
      if (section.kind === 0) return section.document;
    }
    throw new Error('No body in response');
  }

  private async initialFetch(): Promise<void> {
    if (this.started) return;
    this.started = true;

    const reply = await this.sendCommand(this.cmd);
    const cursor = reply.cursor as any;
    if (cursor) {
      this.buffer = cursor.firstBatch ?? [];
      this.cursorId = BigInt(cursor.id ?? 0);
      if (this.cursorId === 0n) this.exhausted = true;
    }
  }

  private async fetchMore(): Promise<void> {
    if (this.exhausted) return;

    const collName = (this.cmd.find ?? this.cmd.aggregate ?? this.cmd.listIndexes ?? '') as string;
    const reply = await this.sendCommand({
      getMore: Number(this.cursorId),
      collection: collName,
      $db: this.dbName,
    });

    const cursor = reply.cursor as any;
    if (cursor) {
      this.buffer = cursor.nextBatch ?? [];
      this.cursorId = BigInt(cursor.id ?? 0);
      if (this.cursorId === 0n) this.exhausted = true;
    } else {
      this.exhausted = true;
    }
  }

  async next(): Promise<BsonDocument | null> {
    await this.initialFetch();
    if (this.buffer.length === 0) {
      await this.fetchMore();
    }
    if (this.buffer.length === 0) return null;
    return this.buffer.shift()!;
  }

  async toArray(): Promise<BsonDocument[]> {
    const docs: BsonDocument[] = [];
    let doc: BsonDocument | null;
    while ((doc = await this.next()) !== null) {
      docs.push(doc);
    }
    return docs;
  }

  async forEach(callback: (doc: BsonDocument) => void | Promise<void>): Promise<void> {
    let doc: BsonDocument | null;
    while ((doc = await this.next()) !== null) {
      await callback(doc);
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<BsonDocument> {
    let done = false;
    const self = this;
    return {
      async next() {
        if (done) return { value: undefined, done: true };
        const doc = await self.next();
        if (doc === null) {
          done = true;
          return { value: undefined as any, done: true };
        }
        return { value: doc, done: false };
      },
    };
  }

  async count(): Promise<number> {
    const docs = await this.toArray();
    return docs.length;
  }

  async close(): Promise<void> {
    if (this.cursorId !== 0n) {
      try {
        const collName = (this.cmd.find ?? this.cmd.aggregate ?? this.cmd.listIndexes ?? '') as string;
        await this.sendCommand({
          killCursors: collName,
          cursors: [Number(this.cursorId)],
          $db: this.dbName,
        });
      } catch {
        // ignore
      }
      this.cursorId = 0n;
    }
  }
}
