import { FindCursor, AggregationCursor, Document } from "mongodb";
import { Sort, Projection } from "./types";

/**
 * MDB Cursor - provides an iterable interface for query results.
 *
 * Supports chaining sort, limit, skip, and projection operations.
 * Implements AsyncIterable for use with `for await...of` loops.
 *
 * @example
 * ```typescript
 * const cursor = users.find({ age: { $gte: 18 } })
 *   .sort({ name: 1 })
 *   .limit(10)
 *   .skip(5);
 *
 * // Iterate with for-await
 * for await (const user of cursor) {
 *   console.log(user.name);
 * }
 *
 * // Or convert to array
 * const results = await cursor.toArray();
 *
 * // Get query explanation
 * const explanation = await cursor.explain();
 * ```
 */
export class MdbCursor<T = Document> implements AsyncIterable<T> {
  private _cursor: FindCursor | AggregationCursor;

  /** @internal */
  constructor(cursor: FindCursor | AggregationCursor) {
    this._cursor = cursor;
  }

  /**
   * Sort the results by the specified field(s).
   * @param sort - Sort specification (e.g., { name: 1 } for ascending).
   */
  sort(sort: Sort<T>): MdbCursor<T> {
    if (this._cursor instanceof FindCursor) {
      return new MdbCursor<T>(this._cursor.sort(sort as any));
    }
    return this;
  }

  /**
   * Limit the number of results returned.
   * @param limit - Maximum number of documents to return.
   */
  limit(limit: number): MdbCursor<T> {
    return new MdbCursor<T>(this._cursor.limit(limit));
  }

  /**
   * Skip a number of results.
   * @param skip - Number of documents to skip.
   */
  skip(skip: number): MdbCursor<T> {
    return new MdbCursor<T>(this._cursor.skip(skip));
  }

  /**
   * Set the batch size for cursor iteration.
   * @param size - Number of documents per batch.
   */
  batchSize(size: number): MdbCursor<T> {
    return new MdbCursor<T>(this._cursor.batchSize(size));
  }

  /**
   * Apply a projection to the results.
   * @param projection - Projection specification (e.g., { name: 1, _id: 0 }).
   */
  projection(projection: Projection): MdbCursor<T> {
    if (this._cursor instanceof FindCursor) {
      return new MdbCursor<T>(this._cursor.project(projection as any));
    }
    return this;
  }

  /**
   * Get the next document from the cursor.
   * @returns The next document, or null if no more documents.
   */
  async next(): Promise<T | null> {
    if (this._cursor.closed) {
      return null;
    }
    return (await this._cursor.next()) as T | null;
  }

  /**
   * Convert the cursor results to an array.
   * @returns Array of all matching documents.
   */
  async toArray(): Promise<T[]> {
    return (await this._cursor.toArray()) as T[];
  }

  /**
   * Get the query explanation (execution plan).
   * @returns Explanation document.
   */
  async explain(): Promise<Document> {
    return (await this._cursor.explain()) as unknown as Document;
  }

  /**
   * Close the cursor and release resources.
   */
  async close(): Promise<void> {
    await this._cursor.close();
  }

  /**
   * Check if the cursor is closed.
   */
  get closed(): boolean {
    return this._cursor.closed;
  }

  /**
   * Get the number of remaining documents (if available).
   */
  get remainingBatchSize(): number {
    if (this._cursor instanceof FindCursor) {
      try {
        return (this._cursor as any).remainingBatchSize();
      } catch {
        return 0;
      }
    }
    return 0;
  }

  /**
   * Async iterator implementation for use with `for await...of`.
   */
  [Symbol.asyncIterator](): AsyncIterator<T> {
    const cursor = this;
    return {
      next: async (): Promise<IteratorResult<T>> => {
        const doc = await cursor.next();
        if (doc === null) {
          return { done: true, value: undefined as any };
        }
        return { done: false, value: doc };
      },
      return: async (): Promise<IteratorResult<T>> => {
        await cursor.close();
        return { done: true, value: undefined as any };
      },
    };
  }
}
