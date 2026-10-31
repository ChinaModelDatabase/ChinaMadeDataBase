import {
  WriteConcern as MongoWriteConcern,
  ReadConcern as MongoReadConcern,
  ReadConcernLevel,
  W as WriteConcernW,
} from "mongodb";

/**
 * Write concern for MDB operations.
 *
 * Controls the level of acknowledgment requested from MDB for write operations.
 */
export class WriteConcern {
  /** The write concern instance. */
  public readonly mongoWriteConcern: MongoWriteConcern;

  /** Majority write concern. */
  public static readonly MAJORITY = new WriteConcern("majority");

  /** Default write concern (acknowledged). */
  public static readonly DEFAULT = new WriteConcern(1);

  /** Unacknowledged write concern. */
  public static readonly UNACKNOWLEDGED = new WriteConcern(0);

  /**
   * Create a new WriteConcern.
   * @param w - Number of nodes to acknowledge, or "majority", or a tag name.
   * @param wtimeoutMS - Timeout in milliseconds for the write concern.
   * @param journal - Whether to wait for journal sync.
   */
  constructor(w?: WriteConcernW, wtimeoutMS?: number, journal?: boolean) {
    this.mongoWriteConcern = new MongoWriteConcern(w, wtimeoutMS, journal);
  }

  /**
   * Create a WriteConcern from a MongoDB WriteConcern instance.
   */
  static fromMongo(wc: MongoWriteConcern): WriteConcern {
    const concern = Object.create(WriteConcern.prototype);
    concern.mongoWriteConcern = wc;
    return concern;
  }

  /** Number of nodes to acknowledge. */
  get w(): WriteConcernW | undefined {
    return this.mongoWriteConcern.w;
  }

  /** Timeout in milliseconds. */
  get wtimeoutMS(): number | undefined {
    return this.mongoWriteConcern.wtimeout;
  }

  /** Whether journal sync is required. */
  get journal(): boolean | undefined {
    return this.mongoWriteConcern.journal;
  }

  /** Whether this write concern is unacknowledged. */
  get isUnacknowledged(): boolean {
    return this.mongoWriteConcern.w === 0;
  }

  /** Convert to a plain object. */
  toDocument(): { w?: WriteConcernW; wtimeoutMS?: number; journal?: boolean } {
    const doc: { w?: WriteConcernW; wtimeoutMS?: number; journal?: boolean } = {};
    if (this.w !== undefined) doc.w = this.w;
    if (this.wtimeoutMS !== undefined) doc.wtimeoutMS = this.wtimeoutMS;
    if (this.journal !== undefined) doc.journal = this.journal;
    return doc;
  }
}

/**
 * Read concern for MDB operations.
 *
 * Controls the consistency and isolation level of read operations.
 */
export class ReadConcern {
  /** The read concern instance. */
  public readonly mongoReadConcern: MongoReadConcern;

  /** Local read concern (default). */
  public static readonly LOCAL = new ReadConcern("local");

  /** Available read concern. */
  public static readonly AVAILABLE = new ReadConcern("available");

  /** Majorit read concern. */
  public static readonly MAJORITY = new ReadConcern("majority");

  /** Linearizable read concern. */
  public static readonly LINEARIZABLE = new ReadConcern("linearizable");

  /** Snapshot read concern (for transactions). */
  public static readonly SNAPSHOT = new ReadConcern("snapshot");

  /**
   * Create a new ReadConcern.
   * @param level - The read concern level.
   */
  constructor(level?: ReadConcernLevel | string) {
    this.mongoReadConcern = new MongoReadConcern(level as ReadConcernLevel);
  }

  /**
   * Create a ReadConcern from a MongoDB ReadConcern instance.
   */
  static fromMongo(rc: MongoReadConcern): ReadConcern {
    const concern = Object.create(ReadConcern.prototype);
    concern.mongoReadConcern = rc;
    return concern;
  }

  /** The read concern level. */
  get level(): string | undefined {
    return this.mongoReadConcern.level;
  }

  /** Convert to a plain object. */
  toDocument(): { level?: string } {
    if (this.level) {
      return { level: this.level };
    }
    return {};
  }
}
