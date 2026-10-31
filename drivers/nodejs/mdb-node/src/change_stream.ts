import { ChangeStream as MongoChangeStream, Document, Timestamp, ResumeToken } from "mongodb";

/**
 * MDB Change Stream - provides real-time monitoring of data changes.
 *
 * Supports resuming, filtering, and iterating over change events.
 *
 * @example
 * ```typescript
 * const changeStream = collection.watch([
 *   { $match: { "operationType": "insert" } }
 * ]);
 *
 * // Iterate over changes
 * for await (const change of changeStream) {
 *   console.log("New document:", change.fullDocument);
 * }
 *
 * // Or get next change manually
 * const change = await changeStream.next();
 *
 * // Resume from a token
 * changeStream.resumeAfter(token);
 * ```
 */
export class MdbChangeStream<T extends Document = Document> {
  private _stream: MongoChangeStream;

  /** @internal */
  constructor(stream: MongoChangeStream) {
    this._stream = stream;
  }

  /**
   * Set the resume token to resume after.
   * @param token - The resume token.
   */
  resumeAfter(token: ResumeToken): this {
    (this._stream as any).resumeAfter(token);
    return this;
  }

  /**
   * Set the resume token to start after.
   * @param token - The resume token.
   */
  startAfter(token: ResumeToken): this {
    (this._stream as any).startAfter(token);
    return this;
  }

  /**
   * Set the operation time to start at.
   * @param time - The operation timestamp.
   */
  startAtOperationTime(time: Timestamp): this {
    (this._stream as any).startAtOperationTime(time);
    return this;
  }

  /**
   * Set the maximum await time for change stream.
   * @param ms - Maximum time in milliseconds to wait for changes.
   */
  maxAwaitTime(ms: number): this {
    (this._stream as any).maxAwaitTimeMS(ms);
    return this;
  }

  /**
   * Get the next change event.
   * @returns The next change document, or null if no more changes.
   */
  async next(): Promise<T | null> {
    if (!(this._stream as any).isAlive) {
      return null;
    }
    return (await this._stream.next()) as unknown as T | null;
  }

  /**
   * Try to get the next change event without waiting.
   * @returns The next change document, or null if no changes are available.
   */
  async tryNext(): Promise<T | null> {
    return (await this._stream.tryNext()) as unknown as T | null;
  }

  /**
   * Close the change stream and release resources.
   */
  async close(): Promise<void> {
    await this._stream.close();
  }

  /**
   * Check if the change stream is alive.
   */
  get isAlive(): boolean {
    return (this._stream as any).isAlive;
  }

  /**
   * Get the current resume token.
   */
  get resumeToken(): ResumeToken | undefined {
    return this._stream.resumeToken;
  }

  /**
   * Async iterator implementation for use with `for await...of`.
   */
  [Symbol.asyncIterator](): AsyncIterator<T> {
    const stream = this;
    return {
      next: async (): Promise<IteratorResult<T>> => {
        const doc = await stream.next();
        if (doc === null) {
          return { done: true, value: undefined as any };
        }
        return { done: false, value: doc };
      },
      return: async (): Promise<IteratorResult<T>> => {
        await stream.close();
        return { done: true, value: undefined as any };
      },
    };
  }
}
