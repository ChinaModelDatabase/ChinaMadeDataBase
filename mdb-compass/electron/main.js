const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const { MongoClient, ObjectId, Binary } = require('mongodb');

let mainWindow;
let mongoClient = null;
let connectedUri = '';

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1000,
    minHeight: 600,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
    backgroundColor: '#1a1a2e',
    title: 'MDB Compass',
    icon: undefined,
  });

  const isDev = process.env.NODE_ENV === 'development' || !app.isPackaged;
  if (isDev) {
    mainWindow.loadURL('http://localhost:3000');
    mainWindow.webContents.openDevTools({ mode: 'detach' });
  } else {
    mainWindow.loadFile(path.join(__dirname, '..', 'build', 'index.html'));
  }

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

app.whenReady().then(createWindow);

app.on('window-all-closed', async () => {
  if (mongoClient) {
    await mongoClient.close();
    mongoClient = null;
  }
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createWindow();
  }
});

function getClient() {
  if (!mongoClient || !mongoClient.topology || !mongoClient.topology.isConnected()) {
    throw new Error('Not connected to database');
  }
  return mongoClient;
}

// Connection
ipcMain.handle('mdb:connect', async (_event, { host, port, username, password, authSource, tls }) => {
  try {
    const auth = username ? `${username}:${password}@` : '';
    const params = [];
    if (authSource) params.push(`authSource=${authSource}`);
    if (tls) params.push('tls=true');
    const qs = params.length ? `?${params.join('&')}` : '';
    connectedUri = `mongodb://${auth}${host}:${port}${qs}`;

    mongoClient = new MongoClient(connectedUri, {
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 5000,
      socketTimeoutMS: 10000,
    });

    await mongoClient.connect();
    await mongoClient.db('admin').command({ ping: 1 });

    return { success: true, host, port };
  } catch (err) {
    return { success: false, error: err.message };
  }
});

ipcMain.handle('mdb:disconnect', async () => {
  try {
    if (mongoClient) {
      await mongoClient.close();
      mongoClient = null;
      connectedUri = '';
    }
    return { success: true };
  } catch (err) {
    return { success: false, error: err.message };
  }
});

ipcMain.handle('mdb:isConnected', async () => {
  try {
    const connected = mongoClient && mongoClient.topology && mongoClient.topology.isConnected();
    return { connected: !!connected };
  } catch {
    return { connected: false };
  }
});

// Database operations
ipcMain.handle('mdb:listDatabases', async () => {
  try {
    const client = getClient();
    const adminDb = client.db('admin');
    const result = await adminDb.command({ listDatabases: 1 });
    return { databases: result.databases };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:listCollections', async (_event, { dbName }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const collections = await db.listCollections().toArray();
    return { collections: collections.map(c => ({ name: c.name, type: c.type, options: c.options })) };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:createCollection', async (_event, { dbName, collectionName, options }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    await db.createCollection(collectionName, options || {});
    return { success: true };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:dropCollection', async (_event, { dbName, collectionName }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    await db.collection(collectionName).drop();
    return { success: true };
  } catch (err) {
    return { error: err.message };
  }
});

// Document operations
ipcMain.handle('mdb:find', async (_event, { dbName, collectionName, filter, options }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const coll = db.collection(collectionName);

    const parsedFilter = parseFilter(filter);
    const opts = {};
    if (options && options.sort) opts.sort = options.sort;
    if (options && options.projection) opts.projection = options.projection;
    if (options && options.limit) opts.limit = options.limit;
    if (options && options.skip) opts.skip = options.skip;

    const cursor = coll.find(parsedFilter, opts);
    const docs = await cursor.toArray();
    const total = await coll.countDocuments(parsedFilter);

    const serialized = docs.map(doc => serializeDoc(doc));
    return { documents: serialized, total };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:insertOne', async (_event, { dbName, collectionName, document }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const result = await db.collection(collectionName).insertOne(document);
    return { insertedId: result.insertedId.toString() };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:insertMany', async (_event, { dbName, collectionName, documents }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const result = await db.collection(collectionName).insertMany(documents);
    return { insertedCount: result.insertedCount, insertedIds: Object.values(result.insertedIds).map(id => id.toString()) };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:updateOne', async (_event, { dbName, collectionName, filter, update }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const parsedFilter = parseFilter(filter);
    const result = await db.collection(collectionName).updateOne(parsedFilter, update);
    return { matchedCount: result.matchedCount, modifiedCount: result.modifiedCount };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:updateMany', async (_event, { dbName, collectionName, filter, update }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const parsedFilter = parseFilter(filter);
    const result = await db.collection(collectionName).updateMany(parsedFilter, update);
    return { matchedCount: result.matchedCount, modifiedCount: result.modifiedCount };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:deleteOne', async (_event, { dbName, collectionName, filter }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const parsedFilter = parseFilter(filter);
    const result = await db.collection(collectionName).deleteOne(parsedFilter);
    return { deletedCount: result.deletedCount };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:deleteMany', async (_event, { dbName, collectionName, filter }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const parsedFilter = parseFilter(filter);
    const result = await db.collection(collectionName).deleteMany(parsedFilter);
    return { deletedCount: result.deletedCount };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:aggregate', async (_event, { dbName, collectionName, pipeline }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const cursor = db.collection(collectionName).aggregate(pipeline, { allowDiskUse: true });
    const results = await cursor.toArray();
    return { results: results.map(doc => serializeDoc(doc)) };
  } catch (err) {
    return { error: err.message };
  }
});

// Index operations
ipcMain.handle('mdb:listIndexes', async (_event, { dbName, collectionName }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const indexes = await db.collection(collectionName).listIndexes().toArray();
    return { indexes: indexes.map(idx => ({ ...idx, _id: idx._id?.toString() })) };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:createIndex', async (_event, { dbName, collectionName, keys, options }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const indexName = await db.collection(collectionName).createIndex(keys, options || {});
    return { indexName };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:dropIndex', async (_event, { dbName, collectionName, indexName }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    await db.collection(collectionName).dropIndex(indexName);
    return { success: true };
  } catch (err) {
    return { error: err.message };
  }
});

// Stats
ipcMain.handle('mdb:dbStats', async (_event, { dbName }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const stats = await db.command({ dbStats: 1 });
    return { stats };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:collStats', async (_event, { dbName, collectionName }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const stats = await db.command({ collStats: collectionName });
    return { stats };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:serverStatus', async () => {
  try {
    const client = getClient();
    const adminDb = client.db('admin');
    const status = await adminDb.command({ serverStatus: 1 });
    return { status };
  } catch (err) {
    return { error: err.message };
  }
});

// Replica set
ipcMain.handle('mdb:replSetGetStatus', async () => {
  try {
    const client = getClient();
    const adminDb = client.db('admin');
    const status = await adminDb.command({ replSetGetStatus: 1 });
    return { status };
  } catch (err) {
    return { error: err.message };
  }
});

// Sharding
ipcMain.handle('mdb:shardStatus', async () => {
  try {
    const client = getClient();
    const adminDb = client.db('admin');
    const status = await adminDb.command({ listShards: 1 });
    return { status };
  } catch (err) {
    return { error: err.message };
  }
});

// GridFS
ipcMain.handle('mdb:gridfsListFiles', async (_event, { dbName, bucketName }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const bucket = db.bucket(bucketName || 'fs');
    const files = await bucket.find({}).toArray();
    return { files: files.map(f => ({ ...f, _id: f._id.toString() })) };
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:gridfsUpload', async (_event, { dbName, bucketName, fileName, fileBuffer, metadata }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const bucket = db.bucket(bucketName || 'fs');
    const uploadStream = bucket.openUploadStream(fileName, { metadata });
    const buffer = Buffer.from(fileBuffer);
    return new Promise((resolve, reject) => {
      uploadStream.on('finish', () => {
        resolve({ fileId: uploadStream.id.toString() });
      });
      uploadStream.on('error', (err) => {
        reject(err);
      });
      uploadStream.end(buffer);
    });
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:gridfsDownload', async (_event, { dbName, bucketName, fileId }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const bucket = db.bucket(bucketName || 'fs');
    const downloadStream = bucket.openDownloadStream(new ObjectId(fileId));
    const chunks = [];
    return new Promise((resolve, reject) => {
      downloadStream.on('data', (chunk) => {
        chunks.push(chunk);
      });
      downloadStream.on('end', () => {
        const buffer = Buffer.concat(chunks);
        resolve({ data: Array.from(buffer) });
      });
      downloadStream.on('error', (err) => {
        reject(err);
      });
    });
  } catch (err) {
    return { error: err.message };
  }
});

ipcMain.handle('mdb:gridfsDelete', async (_event, { dbName, bucketName, fileId }) => {
  try {
    const client = getClient();
    const db = client.db(dbName);
    const bucket = db.bucket(bucketName || 'fs');
    await bucket.delete(new ObjectId(fileId));
    return { success: true };
  } catch (err) {
    return { error: err.message };
  }
});

// File dialog
ipcMain.handle('mdb:openFileDialog', async () => {
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ['openFile'],
  });
  return result;
});

// Helper functions
function parseFilter(filter) {
  if (!filter || typeof filter !== 'string') return filter || {};
  try {
    return JSON.parse(filter);
  } catch {
    return {};
  }
}

function serializeDoc(doc) {
  if (!doc) return doc;
  const result = {};
  for (const [key, value] of Object.entries(doc)) {
    if (value && typeof value === 'object') {
      if (value._bsontype === 'ObjectId') {
        result[key] = { $oid: value.toString() };
      } else if (value._bsontype === 'Binary') {
        result[key] = { $binary: { base64: value.toString('base64'), subType: value.sub_type.toString(16).padStart(2, '0') } };
      } else if (value._bsontype === 'Long') {
        result[key] = { $numberLong: value.toString() };
      } else if (value._bsontype === 'Double') {
        result[key] = value.value;
      } else if (value._bsontype === 'Int32') {
        result[key] = value.value;
      } else if (value._bsontype === 'Decimal128') {
        result[key] = { $numberDecimal: value.toString() };
      } else if (value._bsontype === 'Timestamp') {
        result[key] = { $timestamp: { t: value.high, i: value.low } };
      } else if (value instanceof Date) {
        result[key] = { $date: value.toISOString() };
      } else if (Array.isArray(value)) {
        result[key] = value.map(item => serializeDoc(item));
      } else if (value.constructor && value.constructor.name === 'Object') {
        result[key] = serializeDoc(value);
      } else {
        result[key] = serializeDoc(value);
      }
    } else {
      result[key] = value;
    }
  }
  return result;
}
