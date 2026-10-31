import { ClientSession as MongoClientSession, TransactionOptions as MongoTransactionOptions } from "mongodb";
import { TransactionOptions } from "./types";
import { MdbClient } from "./client";

/**
 * MDB Session - manages a client session for transactions.
 *
 * Sessions provide causal consistency and allow multiple operations
 * to be grouped into a transaction.
 *
 * @example
 * ```typescript
 * const session = client.startSession();
 * try {
 *   await session.startTransaction();
 *   // Perform operations
 *   await session.commitTransaction();
 * } catch (error) {
 *   await session.abortTransaction();
 *   throw error;
 * } finally {
 *   await session.endSession();
 * }
 * ```
 */
export class MdbSession {
  private _client: MdbClient;
  private _session: MongoClientSession;

  /** @internal */
  constructor(client: MdbClient, session: MongoClientSession) {
    this._client = client;
    this._session = session;
  }

  /** The underlying MongoDB session. */
  get native(): MongoClientSession {
    return this._session;
  }

  /** The session ID. */
  get id(): any {
    return this._session.id;
  }

  /**
   * Start a transaction on this session.
   * @param options - Transaction options.
   */
  async startTransaction(options?: TransactionOptions): Promise<void> {
    await this._session.startTransaction(options as MongoTransactionOptions);
  }

  /**
   * Commit the current transaction.
   */
  async commitTransaction(): Promise<void> {
    await this._session.commitTransaction();
  }

  /**
   * Abort the current transaction.
   */
  async abortTransaction(): Promise<void> {
    await this._session.abortTransaction();
  }

  /**
   * End this session and release resources.
   */
  async endSession(): Promise<void> {
    await this._session.endSession();
  }

  /**
   * Get the MDB client associated with this session.
   */
  get client(): MdbClient {
    return this._client;
  }

  /**
   * Check if a transaction is currently in progress.
   */
  get inTransaction(): boolean {
    return this._session.inTransaction();
  }

  /**
   * Get the transaction state.
   */
  get transactionState(): string {
    const state = (this._session as any).transaction?.state;
    return state || "NO_TRANSACTION";
  }
}

/**
 * MDB Transaction - provides a high-level API for database transactions.
 *
 * Wraps MDB session and transaction management into a convenient interface.
 *
 * @example
 * ```typescript
 * const txn = await client.startTransaction();
 * try {
 *   const db = txn.database("mydb");
 *   const users = db.collection("users");
 *   await users.insertOne({ name: "Alice" });
 *   await txn.commit();
 * } catch (error) {
 *   await txn.abort();
 *   throw error;
 * }
 * ```
 */
export class MdbTransaction {
  private _client: MdbClient;
  private _session: MdbSession;

  /** @internal */
  constructor(client: MdbClient, session: MdbSession) {
    this._client = client;
    this._session = session;
  }

  /**
   * Commit the transaction and end the session.
   */
  async commit(): Promise<void> {
    await this._session.commitTransaction();
    await this._session.endSession();
  }

  /**
   * Abort the transaction and end the session.
   */
  async abort(): Promise<void> {
    await this._session.abortTransaction();
    await this._session.endSession();
  }

  /**
   * Get a database within this transaction context.
   * @param name - The database name.
   */
  database(name: string): any {
    return this._client.database(name).withSession(this._session.native);
  }

  /**
   * Get a collection within this transaction context.
   * @param dbName - The database name.
   * @param collName - The collection name.
   */
  collection(dbName: string, collName: string): any {
    return this.database(dbName).collection(collName);
  }

  /** The underlying MDB session. */
  get session(): MdbSession {
    return this._session;
  }

  /** Whether the transaction is still in progress. */
  get isActive(): boolean {
    return this._session.inTransaction;
  }
}
