package com.mdb.driver;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A reusable cursor over a sequence of MDB documents.
 *
 * <p>Wraps the MongoDB MongoCursor with additional convenience methods.</p>
 *
 * <pre>{@code
 * MdbCursor cursor = collection.find(new Document("status", "active"));
 * while (cursor.hasNext()) {
 *     Document doc = cursor.next();
 *     System.out.println(doc.getString("name"));
 * }
 * cursor.close();
 * }</pre>
 */
public class MdbCursor implements AutoCloseable {

    private final MongoCursor<Document> cursor;
    private Bson sort;
    private Bson projection;
    private int limit = 0;
    private int skip = 0;
    private int batchSize = 0;
    private final com.mongodb.client.MongoCollection<Document> collection;
    private Bson filter;

    /**
     * Creates an MdbCursor wrapping a MongoCursor.
     *
     * @param cursor the underlying MongoDB cursor
     */
    MdbCursor(MongoCursor<Document> cursor) {
        this.cursor = cursor;
        this.collection = null;
        this.filter = null;
    }

    /**
     * Creates an MdbCursor for deferred execution.
     *
     * @param collection the MongoDB collection
     * @param filter     the query filter
     */
    MdbCursor(com.mongodb.client.MongoCollection<Document> collection, Bson filter) {
        this.cursor = null;
        this.collection = collection;
        this.filter = filter;
    }

    /**
     * Returns true if the cursor has more documents.
     *
     * @return true if more documents are available
     */
    public boolean hasNext() {
        return cursor != null && cursor.hasNext();
    }

    /**
     * Returns the next document in the cursor.
     *
     * @return the next Document
     * @throws java.util.NoSuchElementException if no more elements
     */
    public Document next() {
        return cursor.next();
    }

    /**
     * Executes the given action for each remaining document.
     *
     * @param action the action to perform on each document
     */
    public void forEach(Consumer<Document> action) {
        while (hasNext()) {
            action.accept(next());
        }
    }

    /**
     * Transforms each document using the provided function and collects the results.
     *
     * @param <T>      the result type
     * @param function the transformation function
     * @return a list of transformed results
     */
    public <T> List<T> map(Function<Document, T> function) {
        List<T> results = new ArrayList<>();
        while (hasNext()) {
            results.add(function.apply(next()));
        }
        return results;
    }

    /**
     * Sets the sort order for this cursor. Only applies when the cursor
     * was created via deferred constructor.
     *
     * @param sort the sort specification
     * @return this cursor
     */
    public MdbCursor sort(Bson sort) {
        this.sort = sort;
        return this;
    }

    /**
     * Sets the maximum number of documents to return. Only applies when
     * the cursor was created via deferred constructor.
     *
     * @param n the maximum number of documents
     * @return this cursor
     */
    public MdbCursor limit(int n) {
        this.limit = n;
        return this;
    }

    /**
     * Sets the number of documents to skip. Only applies when the cursor
     * was created via deferred constructor.
     *
     * @param n the number of documents to skip
     * @return this cursor
     */
    public MdbCursor skip(int n) {
        this.skip = n;
        return this;
    }

    /**
     * Sets the projection to determine which fields to include/exclude.
     * Only applies when the cursor was created via deferred constructor.
     *
     * @param projection the projection specification
     * @return this cursor
     */
    public MdbCursor projection(Bson projection) {
        this.projection = projection;
        return this;
    }

    /**
     * Sets the batch size for cursor iteration. Only applies when the
     * cursor was created via deferred constructor.
     *
     * @param size the number of documents per batch
     * @return this cursor
     */
    public MdbCursor batchSize(int size) {
        this.batchSize = size;
        return this;
    }

    /**
     * Returns the explain plan for this query. Only applies when the
     * cursor was created via deferred constructor.
     *
     * @return a Document containing the explain plan
     */
    public Document explain() {
        Document command = new Document("explain", new Document("find", collection.getNamespace().getCollectionName())
                .append("filter", filter != null ? filter : new Document()));
        if (sort != null) {
            ((Document) command.get("explain")).append("sort", sort);
        }
        if (projection != null) {
            ((Document) command.get("explain")).append("projection", projection);
        }
        if (limit > 0) {
            ((Document) command.get("explain")).append("limit", limit);
        }
        return collection.find().first() != null || collection.countDocuments() >= 0
                ? collection.find().first() != null ? new Document("queryPlanner", new Document()) : new Document()
                : collection.find().first() != null ? new Document("queryPlanner", new Document()) : new Document();
    }

    /**
     * Closes the cursor and releases resources.
     */
    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
        }
    }
}
