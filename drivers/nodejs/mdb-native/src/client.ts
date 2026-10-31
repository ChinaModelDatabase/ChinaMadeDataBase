import { Connection, ConnectionOptions } from './connection';
import { MdbDatabase } from './database';
import { buildOpMsgSingle, nextReqId, OpMsgMessage } from './wire_protocol';
import { BsonDocument } from './bson';

export interface MdbClientOptions extends ConnectionOptions {
  appName?: string;
}

export class MdbClient {
  private conn: Connection;
  private options: MdbClientOptions;
  private adminDb: MdbDatabase | null = null;

  constructor(options: MdbClientOptions) {
    this.options = options;
    this.conn = new Connection(options);
  }

  async connect(): Promise<MdbClient> {
    await this.conn.connect();
    // Optional: send handshake / hello
    try {
      await this.command('admin', { hello: 1 });
    } catch {
      // some servers may not support hello; ignore
    }
    return this;
  }

  db(dbName: string): MdbDatabase {
    return new MdbDatabase(this, dbName);
  }

  get admin(): MdbDatabase {
    if (!this.adminDb) this.adminDb = this.db('admin');
    return this.adminDb;
  }

  async command(dbName: string, cmd: BsonDocument): Promise<BsonDocument> {
    const msg: OpMsgMessage = {
      requestID: nextReqId(),
      responseTo: 0,
      flags: { checksumPresent: false, moreToCome: false, exhaustAllowed: false },
      sections: [
        {
          kind: 0,
          document: { ...cmd, $db: dbName },
        },
      ],
    };

    const reply = await this.conn.send(msg);
    return this.extractResponse(reply);
  }

  private extractResponse(msg: OpMsgMessage): BsonDocument {
    for (const section of msg.sections) {
      if (section.kind === 0) {
        return section.document;
      }
    }
    throw new Error('No body section in response');
  }

  async close(): Promise<void> {
    this.conn.close();
  }

  get isConnected(): boolean {
    return this.conn.isConnected;
  }

  /** Low-level access to the connection for GridFS etc. */
  _getConnection(): Connection {
    return this.conn;
  }
}
