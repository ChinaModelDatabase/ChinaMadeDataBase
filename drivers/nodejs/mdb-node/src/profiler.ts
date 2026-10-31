import { Document, Collection } from "mongodb";
import { MdbDatabase } from "./database";
import { ProfilerLevel } from "./types";

/**
 * MDB Profiler - provides query profiling and slow query analysis.
 *
 * The profiler records information about operations (queries, commands, etc.)
 * exceeding a configurable threshold, helping identify slow queries.
 *
 * @example
 * ```typescript
 * const db = client.database("mydb");
 * const profiler = db.profiler();
 *
 * // Enable profiling for queries slower than 100ms
 * await profiler.enable(100);
 *
 * // Run some queries...
 *
 * // Get slow queries
 * const slowQueries = await profiler.slowQueries();
 *
 * // Get profiler stats
 * const stats = await profiler.stats();
 *
 * // Disable profiling
 * await profiler.disable();
 * ```
 */
export class MdbProfiler {
  private _db: MdbDatabase;

  /** @internal */
  constructor(db: MdbDatabase) {
    this._db = db;
  }

  /**
   * Enable the database profiler.
   * @param slowMs - Queries slower than this threshold (in ms) will be profiled.
   *                 Defaults to 100ms. Use 0 to profile all operations.
   * @param profileLevel - Profiler level (default: 1 = slow queries only).
   */
  async enable(slowMs: number = 100, profileLevel: ProfilerLevel = ProfilerLevel.SlowOnly): Promise<void> {
    await this._db.command({
      profile: profileLevel,
      slowms: slowMs,
    });
  }

  /**
   * Disable the database profiler.
   */
  async disable(): Promise<void> {
    await this._db.command({
      profile: ProfilerLevel.Off,
    });
  }

  /**
   * Get slow queries from the system.profile collection.
   * @param limit - Maximum number of queries to return (default: 100).
   * @returns Array of profiled query documents.
   */
  async slowQueries(limit: number = 100): Promise<Document[]> {
    const db = this._db.native;
    const profileCollection: Collection = db.collection("system.profile");

    const queries = await profileCollection
      .find({})
      .sort({ ts: -1 })
      .limit(limit)
      .toArray();

    return queries as unknown as Document[];
  }

  /**
   * Get profiler statistics.
   * @returns Profiler stats document.
   */
  async stats(): Promise<Document> {
    const result = await this._db.command({ profile: -1 });
    return result as unknown as Document;
  }

  /**
   * Get the current profiler level.
   * @returns The current profiler level.
   */
  async getLevel(): Promise<ProfilerLevel> {
    const result = await this._db.command({ profile: -1 });
    return (result as any).was as ProfilerLevel;
  }

  /**
   * Get the current slow query threshold.
   * @returns The current slowms threshold in milliseconds.
   */
  async getSlowMs(): Promise<number> {
    const result = await this._db.command({ profile: -1 });
    return (result as any).slowms as number;
  }

  /**
   * Clear the system.profile collection.
   */
  async clearProfileData(): Promise<void> {
    const db = this._db.native;
    const profileCollection: Collection = db.collection("system.profile");
    await profileCollection.deleteMany({});
  }

  /**
   * Check if profiling is enabled.
   */
  async isEnabled(): Promise<boolean> {
    const level = await this.getLevel();
    return level !== ProfilerLevel.Off;
  }
}
