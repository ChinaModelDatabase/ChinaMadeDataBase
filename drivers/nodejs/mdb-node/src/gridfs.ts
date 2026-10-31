import {
  GridFSBucket as MongoGridFSBucket,
  ObjectId,
} from "mongodb";
import { Readable, Writable } from "stream";
import { UploadOptions, GridFSFile, Document } from "./types";

/**
 * MDB GridFS Bucket - provides an API for storing and retrieving large files.
 *
 * GridFS splits files into chunks and stores them in a collection,
 * allowing files larger than the 16MB BSON document size limit.
 *
 * @example
 * ```typescript
 * const bucket = collection.gridfsBucket();
 *
 * // Upload from buffer
 * const fileId = await bucket.uploadFromBytes("photo.jpg", imageData, {
 *   contentType: "image/jpeg",
 *   metadata: { album: "vacation" }
 * });
 *
 * // Download to buffer
 * const data = await bucket.downloadToBytes(fileId);
 *
 * // Upload from stream
 * const fileId = await bucket.uploadFromStream("video.mp4", readStream);
 *
 * // List files
 * const files = await bucket.find({ "metadata.album": "vacation" });
 *
 * // Delete
 * await bucket.delete(fileId);
 * ```
 */
export class MdbGridFSBucket {
  private _bucket: MongoGridFSBucket;

  /** @internal */
  constructor(bucket: MongoGridFSBucket) {
    this._bucket = bucket;
  }

  /**
   * Upload a file from a Buffer.
   * @param filename - The name of the file.
   * @param data - The file data as a Buffer.
   * @param options - Upload options.
   * @returns The ObjectId of the uploaded file.
   */
  async uploadFromBytes(
    filename: string,
    data: Buffer,
    options?: UploadOptions
  ): Promise<ObjectId> {
    const uploadStream = this._bucket.openUploadStream(filename, {
      contentType: options?.contentType,
      metadata: options?.metadata,
      chunkSizeBytes: options?.chunkSizeBytes,
    });

    return new Promise<ObjectId>((resolve, reject) => {
      uploadStream.on("error", reject);
      uploadStream.on("finish", () => {
        resolve(uploadStream.id as ObjectId);
      });
      uploadStream.end(data);
    });
  }

  /**
   * Upload a file from a Readable stream.
   * @param filename - The name of the file.
   * @param readable - The readable stream containing file data.
   * @param options - Upload options.
   * @returns The ObjectId of the uploaded file.
   */
  async uploadFromStream(
    filename: string,
    readable: Readable,
    options?: UploadOptions
  ): Promise<ObjectId> {
    const uploadStream = this._bucket.openUploadStream(filename, {
      contentType: options?.contentType,
      metadata: options?.metadata,
      chunkSizeBytes: options?.chunkSizeBytes,
    });

    return new Promise<ObjectId>((resolve, reject) => {
      uploadStream.on("error", reject);
      uploadStream.on("finish", () => {
        resolve(uploadStream.id as ObjectId);
      });
      readable.pipe(uploadStream);
    });
  }

  /**
   * Download a file to a Buffer.
   * @param fileId - The ObjectId of the file.
   * @returns The file data as a Buffer.
   */
  async downloadToBytes(fileId: ObjectId): Promise<Buffer> {
    const downloadStream = this._bucket.openDownloadStream(fileId);

    return new Promise<Buffer>((resolve, reject) => {
      const chunks: Buffer[] = [];
      downloadStream.on("data", (chunk: Buffer) => {
        chunks.push(chunk);
      });
      downloadStream.on("error", reject);
      downloadStream.on("end", () => {
        resolve(Buffer.concat(chunks));
      });
    });
  }

  /**
   * Download a file to a Writable stream.
   * @param fileId - The ObjectId of the file.
   * @param writable - The writable stream to write to.
   */
  async downloadToStream(fileId: ObjectId, writable: Writable): Promise<void> {
    const downloadStream = this._bucket.openDownloadStream(fileId);

    return new Promise<void>((resolve, reject) => {
      downloadStream.on("error", reject);
      writable.on("finish", resolve);
      writable.on("error", reject);
      downloadStream.pipe(writable);
    });
  }

  /**
   * Find files matching a filter.
   * @param filter - Query filter.
   * @returns Array of GridFS file documents.
   */
  async find(filter?: Document): Promise<GridFSFile[]> {
    const files = await this._bucket.find(filter as any).toArray();
    return files as unknown as GridFSFile[];
  }

  /**
   * Delete a file by its ObjectId.
   * @param fileId - The ObjectId of the file to delete.
   */
  async delete(fileId: ObjectId): Promise<void> {
    await this._bucket.delete(fileId);
  }

  /**
   * Rename a file.
   * @param fileId - The ObjectId of the file.
   * @param newFilename - The new filename.
   */
  async rename(fileId: ObjectId, newFilename: string): Promise<void> {
    await this._bucket.rename(fileId, newFilename);
  }

  /**
   * Find a single file by its ObjectId.
   * @param fileId - The ObjectId of the file.
   * @returns The file document or null.
   */
  async findById(fileId: ObjectId): Promise<GridFSFile | null> {
    const files = await this.find({ _id: fileId });
    return files.length > 0 ? files[0] : null;
  }

  /**
   * Find files by filename.
   * @param filename - The filename to search for.
   * @returns Array of matching file documents.
   */
  async findByFilename(filename: string): Promise<GridFSFile[]> {
    return await this.find({ filename });
  }

  /**
   * Delete multiple files matching a filter.
   * @param filter - Query filter.
   */
  async deleteMany(filter: Document): Promise<void> {
    const files = await this.find(filter);
    for (const file of files) {
      await this.delete(file._id);
    }
  }

  /** The underlying MongoDB GridFSBucket instance. */
  get native(): MongoGridFSBucket {
    return this._bucket;
  }
}
