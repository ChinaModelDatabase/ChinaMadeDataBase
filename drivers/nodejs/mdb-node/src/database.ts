import { Db, Document, ClientSession } from "mongodb";
import {
  CreateCollectionOptions,
  Role,
} from "./types";
import { MdbClient } from "./client";
import { MdbCollection } from "./collection";
import { MdbProfiler } from "./profiler";

/**
 * MDB Database - represents a database on the MDB server.
 *
 * Provides methods for collection management, user administration,
 * and database-level operations.
 *
 * @example
 * ```typescript
 * const client = MdbClient.create("localhost", 27017);
 * const db = client.database("mydb");
 *
 * // List collections
 * const collections = await db.listCollections();
 *
 * // Get a collection
 * const users = db.collection("users");
 * ```
 */
export class MdbDatabase {
  private _client: MdbClient;
  private _db: Db;
  private _session?: ClientSession;

  /** @internal */
  constructor(client: MdbClient, db: Db, session?: ClientSession) {
    this._client = client;
    this._db = db;
    this._session = session;
  }

  /** The database name. */
  get name(): string {
    return this._db.databaseName;
  }

  /** The underlying MongoDB Db instance. */
  get native(): Db {
    return this._db;
  }

  /**
   * Create a new MdbDatabase with an associated session for transactions.
   * @param session - The client session.
   */
  withSession(session: ClientSession): MdbDatabase {
    return new MdbDatabase(this._client, this._db, session);
  }

  /**
   * Get a collection reference.
   * @param name - The collection name.
   */
  collection(name: string): MdbCollection {
    return new MdbCollection(this, this._db.collection(name), this._session);
  }

  /**
   * List all collections in this database.
   * @returns Array of collection names.
   */
  async listCollections(): Promise<string[]> {
    const collections = await this._db.listCollections({}, { session: this._session }).toArray();
    return collections.map((c) => c.name);
  }

  /**
   * Create a new collection.
   * @param name - The collection name.
   * @param options - Collection creation options.
   */
  async createCollection(name: string, options?: CreateCollectionOptions): Promise<void> {
    await this._db.createCollection(name, options as any);
  }

  /**
   * Drop a collection.
   * @param name - The collection name.
   */
  async dropCollection(name: string): Promise<void> {
    await this._db.dropCollection(name);
  }

  /**
   * Execute a database command.
   * @param cmd - The command document.
   */
  async command(cmd: Document): Promise<Document> {
    const result = await this._db.command(cmd);
    return result as unknown as Document;
  }

  /**
   * Get database statistics.
   * @returns Database stats document.
   */
  async stats(): Promise<Document> {
    const result = await this._db.command({ dbStats: 1 });
    return result as unknown as Document;
  }

  /**
   * Get the profiler for this database.
   * The profiler tracks slow queries and provides performance insights.
   */
  profiler(): MdbProfiler {
    return new MdbProfiler(this);
  }

  /**
   * Create a new user in this database.
   * @param username - The username.
   * @param password - The password.
   * @param roles - Array of roles to assign (defaults to readWrite).
   */
  async createUser(
    username: string,
    password: string,
    roles?: Role[]
  ): Promise<void> {
    const defaultRoles = roles || [{ role: "readWrite", db: this.name }];
    await this._db.command({
      createUser: username,
      pwd: password,
      roles: defaultRoles,
    });
  }

  /**
   * Drop a user from this database.
   * @param username - The username to drop.
   */
  async dropUser(username: string): Promise<void> {
    await this._db.command({ dropUser: username });
  }

  /**
   * Get all users in this database.
   * @returns Array of user documents.
   */
  async getUsers(): Promise<Document[]> {
    const result = await this._db.command({ usersInfo: 1 });
    return (result as any).users || [];
  }

  /**
   * Grant a role to a user.
   * @param username - The username.
   * @param roles - The roles to grant.
   */
  async grantRolesToUser(username: string, roles: Role[]): Promise<void> {
    await this._db.command({ grantRolesToUser: username, roles });
  }

  /**
   * Revoke a role from a user.
   * @param username - The username.
   * @param roles - The roles to revoke.
   */
  async revokeRolesFromUser(username: string, roles: Role[]): Promise<void> {
    await this._db.command({ revokeRolesFromUser: username, roles });
  }

  /**
   * Get the underlying MDB client.
   */
  get client(): MdbClient {
    return this._client;
  }
}
