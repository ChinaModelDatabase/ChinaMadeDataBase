package com.mdb.driver;

import com.mongodb.client.ClientSession;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * MDB Collection operations wrapper.
 *
 * <p>Provides CRUD, indexing, aggregation, and administrative operations on a collection.</p>
 *
 * <pre>{@code
 * MdbCollection collection = db.getCollection("users");
 * collection.insertOne(new Document("name", "Alice").append("age", 30));
 *
 * MdbCursor cursor = collection.find(new Document("age", new Document("$gte", 25)));
 * while (cursor.hasNext()) {
 *     System.out.println(cursor.next());
 * }
 * cursor.close();
 * }</pre>
 */
public class MdbCollection {

    private final MongoCollection<Document> collection;
    private final ClientSession session;
    private MongoDatabase database;

    /**
     * Creates an MdbCollection without a session.
     *
     * @param collection the underlying MongoDB collection
     */
    MdbCollection(MongoCollection<Document> collection) {
        this(collection, null);
    }

    /**
     * Creates an MdbCollection with a session (for transactions).
     *
     * @param collection the underlying MongoDB collection
     * @param session    the client session (may be null)
     */
    MdbCollection(MongoCollection<Document> collection, ClientSession session) {
        this.collection = collection;
        this.session = session;
    }

    /**
     * Sets the database reference for administrative commands.
     *
     * @param database the MongoDB database
     */
    void setDatabase(MongoDatabase database) {
        this.database = database;
    }

    // ==================== Find Operations ====================

    /**
     * Finds documents matching the filter.
     *
     * @param filter the query filter
     * @return an MdbCursor over the results
     */
    public MdbCursor find(Bson filter) {
        FindIterable<Document> iterable = session != null
                ? collection.find(session, filter)
                : collection.find(filter);
        return new MdbCursor(iterable.iterator());
    }

    /**
     * Finds documents matching the filter with additional options.
     *
     * @param filter     the query filter
     * @param projection the projection specification
     * @return an MdbCursor over the results
     */
    public MdbCursor find(Bson filter, Bson projection) {
        FindIterable<Document> iterable = session != null
                ? collection.find(session, filter).projection(projection)
                : collection.find(filter).projection(projection);
        return new MdbCursor(iterable.iterator());
    }

    /**
     * Returns a single document matching the filter.
     *
     * @param filter the query filter
     * @return the matching Document, or null if not found
     */
    public Document findOne(Bson filter) {
        return session != null
                ? collection.find(session, filter).first()
                : collection.find(filter).first();
    }

    /**
     * Returns a single document matching the filter with projection.
     *
     * @param filter     the query filter
     * @param projection the projection specification
     * @return the matching Document, or null if not found
     */
    public Document findOne(Bson filter, Bson projection) {
        FindIterable<Document> iterable = session != null
                ? collection.find(session, filter).projection(projection)
                : collection.find(filter).projection(projection);
        return iterable.first();
    }

    // ==================== Insert Operations ====================

    /**
     * Inserts a single document.
     *
     * @param document the document to insert
     * @return the inserted document's _id
     */
    public Object insertOne(Document document) {
        if (session != null) {
            collection.insertOne(session, document);
        } else {
            collection.insertOne(document);
        }
        return document.get("_id");
    }

    /**
     * Inserts multiple documents.
     *
     * @param documents the documents to insert
     */
    public void insertMany(List<Document> documents) {
        if (session != null) {
            collection.insertMany(session, documents);
        } else {
            collection.insertMany(documents);
        }
    }

    // ==================== Update Operations ====================

    /**
     * Updates a single document matching the filter.
     *
     * @param filter the query filter
     * @param update the update operations
     * @return the number of documents modified
     */
    public long updateOne(Bson filter, Bson update) {
        return session != null
                ? collection.updateOne(session, filter, update).getModifiedCount()
                : collection.updateOne(filter, update).getModifiedCount();
    }

    /**
     * Updates all documents matching the filter.
     *
     * @param filter the query filter
     * @param update the update operations
     * @return the number of documents modified
     */
    public long updateMany(Bson filter, Bson update) {
        return session != null
                ? collection.updateMany(session, filter, update).getModifiedCount()
                : collection.updateMany(filter, update).getModifiedCount();
    }

    /**
     * Replaces a single document matching the filter.
     *
     * @param filter      the query filter
     * @param replacement the replacement document
     * @return the number of documents replaced
     */
    public long replaceOne(Bson filter, Document replacement) {
        return session != null
                ? collection.replaceOne(session, filter, replacement).getModifiedCount()
                : collection.replaceOne(filter, replacement).getModifiedCount();
    }

    // ==================== Delete Operations ====================

    /**
     * Deletes a single document matching the filter.
     *
     * @param filter the query filter
     * @return the number of documents deleted
     */
    public long deleteOne(Bson filter) {
        return session != null
                ? collection.deleteOne(session, filter).getDeletedCount()
                : collection.deleteOne(filter).getDeletedCount();
    }

    /**
     * Deletes all documents matching the filter.
     *
     * @param filter the query filter
     * @return the number of documents deleted
     */
    public long deleteMany(Bson filter) {
        return session != null
                ? collection.deleteMany(session, filter).getDeletedCount()
                : collection.deleteMany(filter).getDeletedCount();
    }

    // ==================== Find and Modify ====================

    /**
     * Finds a single document and modifies it atomically.
     *
     * @param query   the query filter
     * @param update  the update operations
     * @param options the find-and-modify options
     * @return the original or modified document, or null if not found
     */
    public Document findAndModify(Bson query, Bson update, Document options) {
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        if (options != null) {
            if (options.containsKey("upsert")) {
                opts.upsert(options.getBoolean("upsert"));
            }
            if (options.containsKey("sort")) {
                opts.sort((Bson) options.get("sort"));
            }
            if (options.containsKey("returnDocument")) {
                String returnDoc = options.getString("returnDocument");
                if ("after".equalsIgnoreCase(returnDoc)) {
                    opts.returnDocument(ReturnDocument.AFTER);
                } else {
                    opts.returnDocument(ReturnDocument.BEFORE);
                }
            }
            if (options.containsKey("arrayFilters")) {
                @SuppressWarnings("unchecked")
                List<Bson> arrayFilters = (List<Bson>) options.get("arrayFilters");
                opts.arrayFilters(arrayFilters);
            }
        }
        return session != null
                ? collection.findOneAndUpdate(session, query, update, opts)
                : collection.findOneAndUpdate(query, update, opts);
    }

    // ==================== Aggregation ====================

    /**
     * Runs an aggregation pipeline.
     *
     * @param pipeline the aggregation pipeline stages
     * @return an MdbCursor over the aggregation results
     */
    public MdbCursor aggregate(List<Document> pipeline) {
        List<Bson> bsonPipeline = new ArrayList<>(pipeline);
        return session != null
                ? new MdbCursor(collection.aggregate(session, bsonPipeline).iterator())
                : new MdbCursor(collection.aggregate(bsonPipeline).iterator());
    }

    // ==================== Distinct ====================

    /**
     * Returns distinct values for a key across the collection.
     *
     * @param key the field name
     * @return a list of distinct values
     */
    public <T> List<T> distinct(String key) {
        return session != null
                ? collection.distinct(session, key).into(new ArrayList<>())
                : collection.distinct(key).into(new ArrayList<>());
    }

    /**
     * Returns distinct values for a key matching the filter.
     *
     * @param key    the field name
     * @param filter the query filter
     * @return a list of distinct values
     */
    public <T> List<T> distinct(String key, Bson filter) {
        return session != null
                ? collection.distinct(session, key, filter).into(new ArrayList<>())
                : collection.distinct(key, filter).into(new ArrayList<>());
    }

    // ==================== Count ====================

    /**
     * Counts all documents in the collection.
     *
     * @return the document count
     */
    public long count() {
        return session != null
                ? collection.countDocuments(session)
                : collection.countDocuments();
    }

    /**
     * Counts documents matching the filter.
     *
     * @param filter the query filter
     * @return the matching document count
     */
    public long count(Bson filter) {
        return session != null
                ? collection.countDocuments(session, filter)
                : collection.countDocuments(filter);
    }

    // ==================== Index Operations ====================

    /**
     * Creates an index on the specified keys.
     *
     * @param keys the index keys
     * @return the index name
     */
    public String createIndex(Bson keys) {
        return session != null
                ? collection.createIndex(session, keys)
                : collection.createIndex(keys);
    }

    /**
     * Creates an index with the specified options.
     *
     * @param keys    the index keys
     * @param options the index options
     * @return the index name
     */
    public String createIndex(Bson keys, IndexOptions options) {
        return session != null
                ? collection.createIndex(session, keys, options)
                : collection.createIndex(keys, options);
    }

    /**
     * Drops the index with the given name.
     *
     * @param indexName the index name
     */
    public void dropIndex(String indexName) {
        collection.dropIndex(indexName);
    }

    /**
     * Returns a list of all indexes on this collection.
     *
     * @return a list of index documents
     */
    public List<Document> listIndexes() {
        List<Document> indexes = new ArrayList<>();
        for (Document doc : collection.listIndexes()) {
            indexes.add(doc);
        }
        return indexes;
    }

    // ==================== Collection Administration ====================

    /**
     * Returns collection statistics.
     *
     * @return a Document containing collection stats
     */
    public Document getStats() {
        MongoDatabase db = getDatabase();
        return db.runCommand(new Document("collStats", collection.getNamespace().getCollectionName()));
    }

    /**
     * Validates the collection.
     *
     * @return a Document containing validation results
     */
    public Document validate() {
        MongoDatabase db = getDatabase();
        return db.runCommand(new Document("validate", collection.getNamespace().getCollectionName()));
    }

    /**
     * Compacts the collection to reclaim storage space.
     */
    public void compact() {
        MongoDatabase db = getDatabase();
        db.runCommand(new Document("compact", collection.getNamespace().getCollectionName()));
    }

    /**
     * Rebuilds all indexes on this collection.
     */
    public void reIndex() {
        MongoDatabase db = getDatabase();
        db.runCommand(new Document("reIndex", collection.getNamespace().getCollectionName()));
    }

    // ==================== Change Streams ====================

    /**
     * Opens a change stream on this collection.
     *
     * @return an MdbChangeStream
     */
    public MdbChangeStream watch() {
        ChangeStreamIterable<Document> iterable = session != null
                ? collection.watch(session)
                : collection.watch();
        return new MdbChangeStream(iterable);
    }

    /**
     * Opens a change stream with an initial pipeline filter.
     *
     * @param pipeline the aggregation pipeline stages to filter events
     * @return an MdbChangeStream
     */
    public MdbChangeStream watch(List<Document> pipeline) {
        List<Bson> bsonPipeline = new ArrayList<>(pipeline);
        ChangeStreamIterable<Document> iterable = session != null
                ? collection.watch(session, bsonPipeline)
                : collection.watch(bsonPipeline);
        return new MdbChangeStream(iterable);
    }

    // ==================== GridFS ====================

    /**
     * Returns a GridFSBucket for this collection's database using the default
     * bucket name ("fs").
     *
     * @return an MdbGridFSBucket
     */
    public MdbGridFSBucket getGridFSBucket() {
        MongoDatabase db = getDatabase();
        return MdbGridFSBucket.create(new MdbDatabase(null, db));
    }

    /**
     * Returns the collection name.
     *
     * @return the collection name
     */
    public String getName() {
        return collection.getNamespace().getCollectionName();
    }

    /**
     * Returns the underlying MongoDB collection.
     *
     * @return the MongoCollection
     */
    public MongoCollection<Document> getUnderlyingCollection() {
        return collection;
    }

    private MongoDatabase getDatabase() {
        if (database != null) {
            return database;
        }
        // Fallback: obtain the database from the namespace
        String dbName = collection.getNamespace().getDatabaseName();
        // We cannot get a MongoDatabase without a MongoClient reference,
        // so use runCommand via the collection's internal client
        try {
            java.lang.reflect.Method getMongoMethod = collection.getClass().getMethod("getMongo");
            Object mongoClient = getMongoMethod.invoke(collection);
            java.lang.reflect.Method getDatabaseMethod = mongoClient.getClass().getMethod("getDatabase", String.class);
            return (MongoDatabase) getDatabaseMethod.invoke(mongoClient, dbName);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot access database. Pass MdbDatabase to MdbCollection.", e);
        }
    }
}
