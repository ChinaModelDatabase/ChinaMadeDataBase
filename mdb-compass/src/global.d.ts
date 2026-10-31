declare global {
  interface Window {
    mdb: {
      connect: (config: { host: string; port: number; username?: string; password?: string; authSource?: string; tls?: boolean }) => Promise<{ success?: boolean; error?: string; host?: string; port?: number }>;
      disconnect: () => Promise<{ success?: boolean; error?: string }>;
      isConnected: () => Promise<{ connected: boolean }>;
      listDatabases: () => Promise<{ databases?: Array<{ name: string; sizeOnDisk: number; empty: boolean }>; error?: string }>;
      listCollections: (dbName: string) => Promise<{ collections?: Array<{ name: string; type: string; options: Record<string, unknown> }>; error?: string }>;
      createCollection: (dbName: string, collectionName: string, options?: Record<string, unknown>) => Promise<{ success?: boolean; error?: string }>;
      dropCollection: (dbName: string, collectionName: string) => Promise<{ success?: boolean; error?: string }>;
      find: (dbName: string, collectionName: string, filter?: string, options?: { sort?: Record<string, 1 | -1>; projection?: Record<string, 1 | 0>; limit?: number; skip?: number }) => Promise<{ documents?: Record<string, unknown>[]; total?: number; error?: string }>;
      insertOne: (dbName: string, collectionName: string, document: Record<string, unknown>) => Promise<{ insertedId?: string; error?: string }>;
      insertMany: (dbName: string, collectionName: string, documents: Record<string, unknown>[]) => Promise<{ insertedCount?: number; insertedIds?: string[]; error?: string }>;
      updateOne: (dbName: string, collectionName: string, filter: string, update: Record<string, unknown>) => Promise<{ matchedCount?: number; modifiedCount?: number; error?: string }>;
      updateMany: (dbName: string, collectionName: string, filter: string, update: Record<string, unknown>) => Promise<{ matchedCount?: number; modifiedCount?: number; error?: string }>;
      deleteOne: (dbName: string, collectionName: string, filter: string) => Promise<{ deletedCount?: number; error?: string }>;
      deleteMany: (dbName: string, collectionName: string, filter: string) => Promise<{ deletedCount?: number; error?: string }>;
      aggregate: (dbName: string, collectionName: string, pipeline: Record<string, unknown>[]) => Promise<{ results?: Record<string, unknown>[]; error?: string }>;
      listIndexes: (dbName: string, collectionName: string) => Promise<{ indexes?: Array<{ name: string; key: Record<string, unknown>; unique?: boolean; sparse?: boolean; expireAfterSeconds?: number }>; error?: string }>;
      createIndex: (dbName: string, collectionName: string, keys: Record<string, 1 | -1>, options?: Record<string, unknown>) => Promise<{ indexName?: string; error?: string }>;
      dropIndex: (dbName: string, collectionName: string, indexName: string) => Promise<{ success?: boolean; error?: string }>;
      dbStats: (dbName: string) => Promise<{ stats?: Record<string, unknown>; error?: string }>;
      collStats: (dbName: string, collectionName: string) => Promise<{ stats?: Record<string, unknown>; error?: string }>;
      serverStatus: () => Promise<{ status?: Record<string, unknown>; error?: string }>;
      replSetGetStatus: () => Promise<{ status?: Record<string, unknown>; error?: string }>;
      shardStatus: () => Promise<{ status?: Record<string, unknown>; error?: string }>;
      gridfsListFiles: (dbName: string, bucketName?: string) => Promise<{ files?: Array<{ _id: string; filename: string; length: number; chunkSize: number; uploadDate: string; metadata?: Record<string, unknown> }>; error?: string }>;
      gridfsUpload: (dbName: string, bucketName: string | undefined, fileName: string, fileBuffer: ArrayBuffer, metadata?: Record<string, unknown>) => Promise<{ fileId?: string; error?: string }>;
      gridfsDownload: (dbName: string, bucketName: string | undefined, fileId: string) => Promise<{ data?: number[]; error?: string }>;
      gridfsDelete: (dbName: string, bucketName: string | undefined, fileId: string) => Promise<{ success?: boolean; error?: string }>;
      openFileDialog: () => Promise<{ canceled: boolean; filePaths: string[] }>;
    };
  }
}

export {};
