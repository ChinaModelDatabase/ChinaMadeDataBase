const { contextBridge, ipcRenderer } = require('electron');

const api = {
  connect: (config) => ipcRenderer.invoke('mdb:connect', config),
  disconnect: () => ipcRenderer.invoke('mdb:disconnect'),
  isConnected: () => ipcRenderer.invoke('mdb:isConnected'),

  listDatabases: () => ipcRenderer.invoke('mdb:listDatabases'),
  listCollections: (dbName) => ipcRenderer.invoke('mdb:listCollections', { dbName }),
  createCollection: (dbName, collectionName, options) =>
    ipcRenderer.invoke('mdb:createCollection', { dbName, collectionName, options }),
  dropCollection: (dbName, collectionName) =>
    ipcRenderer.invoke('mdb:dropCollection', { dbName, collectionName }),

  find: (dbName, collectionName, filter, options) =>
    ipcRenderer.invoke('mdb:find', { dbName, collectionName, filter, options }),
  insertOne: (dbName, collectionName, document) =>
    ipcRenderer.invoke('mdb:insertOne', { dbName, collectionName, document }),
  insertMany: (dbName, collectionName, documents) =>
    ipcRenderer.invoke('mdb:insertMany', { dbName, collectionName, documents }),
  updateOne: (dbName, collectionName, filter, update) =>
    ipcRenderer.invoke('mdb:updateOne', { dbName, collectionName, filter, update }),
  updateMany: (dbName, collectionName, filter, update) =>
    ipcRenderer.invoke('mdb:updateMany', { dbName, collectionName, filter, update }),
  deleteOne: (dbName, collectionName, filter) =>
    ipcRenderer.invoke('mdb:deleteOne', { dbName, collectionName, filter }),
  deleteMany: (dbName, collectionName, filter) =>
    ipcRenderer.invoke('mdb:deleteMany', { dbName, collectionName, filter }),
  aggregate: (dbName, collectionName, pipeline) =>
    ipcRenderer.invoke('mdb:aggregate', { dbName, collectionName, pipeline }),

  listIndexes: (dbName, collectionName) =>
    ipcRenderer.invoke('mdb:listIndexes', { dbName, collectionName }),
  createIndex: (dbName, collectionName, keys, options) =>
    ipcRenderer.invoke('mdb:createIndex', { dbName, collectionName, keys, options }),
  dropIndex: (dbName, collectionName, indexName) =>
    ipcRenderer.invoke('mdb:dropIndex', { dbName, collectionName, indexName }),

  dbStats: (dbName) => ipcRenderer.invoke('mdb:dbStats', { dbName }),
  collStats: (dbName, collectionName) =>
    ipcRenderer.invoke('mdb:collStats', { dbName, collectionName }),
  serverStatus: () => ipcRenderer.invoke('mdb:serverStatus'),

  replSetGetStatus: () => ipcRenderer.invoke('mdb:replSetGetStatus'),
  shardStatus: () => ipcRenderer.invoke('mdb:shardStatus'),

  gridfsListFiles: (dbName, bucketName) =>
    ipcRenderer.invoke('mdb:gridfsListFiles', { dbName, bucketName }),
  gridfsUpload: (dbName, bucketName, fileName, fileBuffer, metadata) =>
    ipcRenderer.invoke('mdb:gridfsUpload', { dbName, bucketName, fileName, fileBuffer, metadata }),
  gridfsDownload: (dbName, bucketName, fileId) =>
    ipcRenderer.invoke('mdb:gridfsDownload', { dbName, bucketName, fileId }),
  gridfsDelete: (dbName, bucketName, fileId) =>
    ipcRenderer.invoke('mdb:gridfsDelete', { dbName, bucketName, fileId }),

  openFileDialog: () => ipcRenderer.invoke('mdb:openFileDialog'),
};

contextBridge.exposeInMainWorld('mdb', api);

export type MdbApi = typeof api;
