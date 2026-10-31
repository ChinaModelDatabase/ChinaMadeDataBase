export const mdbService = {
  async connect(host: string, port: number, username?: string, password?: string, authSource?: string, tls?: boolean) {
    return window.mdb.connect({ host, port, username, password, authSource, tls });
  },

  async disconnect() {
    return window.mdb.disconnect();
  },

  async isConnected() {
    return window.mdb.isConnected();
  },

  async listDatabases() {
    return window.mdb.listDatabases();
  },

  async listCollections(dbName: string) {
    return window.mdb.listCollections(dbName);
  },

  async createCollection(dbName: string, collectionName: string, options?: Record<string, unknown>) {
    return window.mdb.createCollection(dbName, collectionName, options);
  },

  async dropCollection(dbName: string, collectionName: string) {
    return window.mdb.dropCollection(dbName, collectionName);
  },

  async find(dbName: string, collectionName: string, filter?: string, options?: { sort?: Record<string, 1 | -1>; projection?: Record<string, 1 | 0>; limit?: number; skip?: number }) {
    return window.mdb.find(dbName, collectionName, filter, options);
  },

  async insertOne(dbName: string, collectionName: string, document: Record<string, unknown>) {
    return window.mdb.insertOne(dbName, collectionName, document);
  },

  async insertMany(dbName: string, collectionName: string, documents: Record<string, unknown>[]) {
    return window.mdb.insertMany(dbName, collectionName, documents);
  },

  async updateOne(dbName: string, collectionName: string, filter: string, update: Record<string, unknown>) {
    return window.mdb.updateOne(dbName, collectionName, filter, update);
  },

  async updateMany(dbName: string, collectionName: string, filter: string, update: Record<string, unknown>) {
    return window.mdb.updateMany(dbName, collectionName, filter, update);
  },

  async deleteOne(dbName: string, collectionName: string, filter: string) {
    return window.mdb.deleteOne(dbName, collectionName, filter);
  },

  async deleteMany(dbName: string, collectionName: string, filter: string) {
    return window.mdb.deleteMany(dbName, collectionName, filter);
  },

  async aggregate(dbName: string, collectionName: string, pipeline: Record<string, unknown>[]) {
    return window.mdb.aggregate(dbName, collectionName, pipeline);
  },

  async listIndexes(dbName: string, collectionName: string) {
    return window.mdb.listIndexes(dbName, collectionName);
  },

  async createIndex(dbName: string, collectionName: string, keys: Record<string, 1 | -1>, options?: Record<string, unknown>) {
    return window.mdb.createIndex(dbName, collectionName, keys, options);
  },

  async dropIndex(dbName: string, collectionName: string, indexName: string) {
    return window.mdb.dropIndex(dbName, collectionName, indexName);
  },

  async dbStats(dbName: string) {
    return window.mdb.dbStats(dbName);
  },

  async collStats(dbName: string, collectionName: string) {
    return window.mdb.collStats(dbName, collectionName);
  },

  async serverStatus() {
    return window.mdb.serverStatus();
  },

  async replSetGetStatus() {
    return window.mdb.replSetGetStatus();
  },

  async shardStatus() {
    return window.mdb.shardStatus();
  },

  async gridfsListFiles(dbName: string, bucketName?: string) {
    return window.mdb.gridfsListFiles(dbName, bucketName);
  },

  async gridfsUpload(dbName: string, bucketName: string | undefined, fileName: string, fileBuffer: ArrayBuffer, metadata?: Record<string, unknown>) {
    return window.mdb.gridfsUpload(dbName, bucketName, fileName, fileBuffer, metadata);
  },

  async gridfsDownload(dbName: string, bucketName: string | undefined, fileId: string) {
    return window.mdb.gridfsDownload(dbName, bucketName, fileId);
  },

  async gridfsDelete(dbName: string, bucketName: string | undefined, fileId: string) {
    return window.mdb.gridfsDelete(dbName, bucketName, fileId);
  },
};
