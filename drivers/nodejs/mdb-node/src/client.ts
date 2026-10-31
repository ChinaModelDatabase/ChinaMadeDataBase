import {
  MongoClient,
  Db,
  ClientSession,
  Document,
} from "mongodb";
import {
  ClientOptions,
  SessionOptions,
  TransactionOptions,
} from "./types";
import { MdbDatabase } from "./database";
import { MdbSession, MdbTransaction } from "./transaction";

/**
 * MDB Client - main entry point for connecting to an MDB server.
 *
 * Wraps the MongoDB driver's MongoClient with MDB-specific extensions
 * and a convenient API.
 *
 * @example
 * ```typescript
 * // Basic connection
 * const client = MdbClient.create("localhost", 27017);
 * await client.connect();
 *
 * // Using URI
 * const client = MdbClient.fromURI("mongodb://localhost:27017/mydb");
 *
 * // With authentication
 * const client = MdbClient.createWithAuth("localhost", 27017, "admin", "secret");
 * ```
 */
export class MdbClient {
  private _client: MongoClient;
  private _connected: boolean = false;

  /**
   * Create an MdbClient with host and port.
   * @param host - The server hostname.
   * @param port - The server port.
   * @param options - Additional client options.
   */
  static create(host: string, port: number, options?: ClientOptions): MdbClient {
    const uri = `mongodb://${host}:${port}`;
    return MdbClient.fromURI(uri, options);
  }

  /**
   * Create an MdbClient from a connection URI.
   * @param uri - MongoDB connection URI (e.g., "mongodb://host:port/db").
   * @param options - Additional client options.
   */
  static fromURI(uri: string, options?: ClientOptions): MdbClient {
    const mongoOptions: any = { ...options?.mongoOptions };

    if (options?.appName) mongoOptions.appName = options.appName;
    if (options?.authSource) mongoOptions.authSource = options.authSource;
    if (options?.replicaSet) mongoOptions.replicaSet = options.replicaSet;
    if (options?.directConnection !== undefined) mongoOptions.directConnection = options.directConnection;
    if (options?.connectTimeoutMS !== undefined) mongoOptions.connectTimeoutMS = options.connectTimeoutMS;
    if (options?.socketTimeoutMS !== undefined) mongoOptions.socketTimeoutMS = options.socketTimeoutMS;
    if (options?.serverSelectionTimeoutMS !== undefined) mongoOptions.serverSelectionTimeoutMS = options.serverSelectionTimeoutMS;
    if (options?.maxPoolSize !== undefined) mongoOptions.maxPoolSize = options.maxPoolSize;
    if (options?.minPoolSize !== undefined) mongoOptions.minPoolSize = options.minPoolSize;
    if (options?.maxIdleTimeMS !== undefined) mongoOptions.maxIdleTimeMS = options.maxIdleTimeMS;
    if (options?.readPreference) mongoOptions.readPreference = options.readPreference;
    if (options?.compressors) mongoOptions.compressors = options.compressors;
    if (options?.retryWrites !== undefined) mongoOptions.retryWrites = options.retryWrites;
    if (options?.retryReads !== undefined) mongoOptions.retryReads = options.retryReads;
    if (options?.tls !== undefined) mongoOptions.tls = options.tls;
    if (options?.tlsCertificateKeyFile) mongoOptions.tlsCertificateKeyFile = options.tlsCertificateKeyFile;
    if (options?.tlsCAFile) mongoOptions.tlsCAFile = options.tlsCAFile;
    if (options?.tlsAllowInvalidCertificates !== undefined) mongoOptions.tlsAllowInvalidCertificates = options.tlsAllowInvalidCertificates;

    const client = new MongoClient(uri, mongoOptions);
    return new MdbClient(client);
  }

  /**
   * Create an MdbClient with authentication credentials.
   * @param host - The server hostname.
   * @param port - The server port.
   * @param username - The authentication username.
   * @param password - The authentication password.
   * @param authDb - The authentication database (defaults to "admin").
   */
  static createWithAuth(
    host: string,
    port: number,
    username: string,
    password: string,
    authDb: string = "admin"
  ): MdbClient {
    const uri = `mongodb://${username}:${encodeURIComponent(password)}@${host}:${port}/?authSource=${authDb}`;
    return MdbClient.fromURI(uri);
  }

  private constructor(client: MongoClient) {
    this._client = client;
  }

  /**
   * Connect to the MDB server.
   * Must be called before performing any operations.
   */
  async connect(): Promise<void> {
    if (!this._connected) {
      await this._client.connect();
      this._connected = true;
    }
  }

  /**
   * Get a database reference.
   * @param name - The database name.
   */
  database(name: string): MdbDatabase {
    return new MdbDatabase(this, this._client.db(name));
  }

  /**
   * List all databases on the server.
   * @returns Array of database names.
   */
  async listDatabases(): Promise<string[]> {
    await this.ensureConnected();
    const admin = this._client.db().admin();
    const result = await admin.listDatabases();
    return result.databases.map((db) => db.name);
  }

  /**
   * Get server status information.
   * @returns Server status document.
   */
  async serverStatus(): Promise<Document> {
    await this.ensureConnected();
    const admin = this._client.db().admin();
    const result = await admin.command({ serverStatus: 1 });
    return result as unknown as Document;
  }

  /**
   * Get replica set status information.
   * Only works when connected to a replica set.
   * @returns Replica set status document.
   */
  async replicaSetStatus(): Promise<Document> {
    await this.ensureConnected();
    const admin = this._client.db().admin();
    const result = await admin.command({ replSetGetStatus: 1 });
    return result as unknown as Document;
  }

  /**
   * Check if the client is currently connected.
   */
  isConnected(): boolean {
    if (!this._connected) return false;
    try {
      this._client.db().command({ ping: 1 });
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Close the client connection and release resources.
   */
  async close(): Promise<void> {
    if (this._connected) {
      await this._client.close();
      this._connected = false;
    }
  }

  /**
   * Start a new client session for transactions.
   * @param options - Session options.
   */
  startSession(options?: SessionOptions): MdbSession {
    const mongoSession = this._client.startSession(options);
    return new MdbSession(this, mongoSession);
  }

  /**
   * Start a new transaction within a session.
   * @param options - Transaction options.
   * @returns A transaction handle.
   */
  async startTransaction(options?: TransactionOptions): Promise<MdbTransaction> {
    const session = this.startSession();
    await session.startTransaction(options);
    return new MdbTransaction(this, session);
  }

  /**
   * Get the underlying MongoDB MongoClient.
   */
  get native(): MongoClient {
    return this._client;
  }

  /** @internal */
  async ensureConnected(): Promise<void> {
    if (!this._connected) {
      await this.connect();
    }
  }

  /**
   * Async dispose support.
   */
  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }
}
