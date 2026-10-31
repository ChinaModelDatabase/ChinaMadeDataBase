package com.mdb.driver;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * MDB Change Stream for receiving real-time notifications of data changes.
 *
 * <p>Wraps the MongoDB Change Stream with MDB-specific convenience methods.</p>
 *
 * <pre>{@code
 * MdbChangeStream stream = collection.watch();
 * stream.forEach(event -> {
 *     System.out.println("Operation: " + event.getOperationType());
 *     System.out.println("Document: " + event.getFullDocument());
 * });
 * }</pre>
 */
public class MdbChangeStream implements AutoCloseable {

    private final ChangeStreamIterable<Document> iterable;
    private MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
    private BsonValue resumeAfter;
    private BsonValue startAfter;
    private BsonTimestamp startAtOperationTime;
    private long maxAwaitTimeMs = -1;
    private final List<Document> pipeline;

    /**
     * Creates a new MdbChangeStream.
     *
     * @param iterable the underlying MongoDB change stream iterable
     */
    MdbChangeStream(ChangeStreamIterable<Document> iterable) {
        this.iterable = iterable;
        this.pipeline = new ArrayList<>();
    }

    /**
     * Returns true if the stream has more events.
     *
     * @return true if more events are available
     */
    public boolean hasNext() {
        ensureCursor();
        return cursor.hasNext();
    }

    /**
     * Returns the next change stream event.
     *
     * @return the next ChangeStreamDocument
     * @throws java.util.NoSuchElementException if no more events
     */
    public ChangeStreamDocument<Document> next() {
        ensureCursor();
        return cursor.next();
    }

    /**
     * Executes the given action for each remaining event.
     *
     * @param action the action to perform on each event
     */
    public void forEach(java.util.function.Consumer<ChangeStreamDocument<Document>> action) {
        while (hasNext()) {
            action.accept(next());
        }
    }

    /**
     * Configures the change stream to resume after the given resume token.
     *
     * @param token the resume token (BsonDocument or BsonValue)
     * @return this change stream
     */
    public MdbChangeStream resumeAfter(BsonValue token) {
        this.resumeAfter = token;
        this.cursor = null;
        return this;
    }

    /**
     * Configures the change stream to start after the given resume token.
     *
     * @param token the resume token
     * @return this change stream
     */
    public MdbChangeStream startAfter(BsonValue token) {
        this.startAfter = token;
        this.cursor = null;
        return this;
    }

    /**
     * Configures the change stream to start at the given operation time.
     *
     * @param time the operation timestamp
     * @return this change stream
     */
    public MdbChangeStream startAtOperationTime(BsonTimestamp time) {
        this.startAtOperationTime = time;
        this.cursor = null;
        return this;
    }

    /**
     * Sets the maximum amount of time the server will wait for new changes.
     *
     * @param timeMs the maximum await time in milliseconds
     * @return this change stream
     */
    public MdbChangeStream maxAwaitTime(long timeMs) {
        this.maxAwaitTimeMs = timeMs;
        this.cursor = null;
        return this;
    }

    /**
     * Adds pipeline stages to filter change events.
     *
     * @param stages the aggregation pipeline stages
     * @return this change stream
     */
    public MdbChangeStream pipeline(List<Document> stages) {
        this.pipeline.addAll(stages);
        this.cursor = null;
        return this;
    }

    /**
     * Closes the change stream.
     */
    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
        }
    }

    private void ensureCursor() {
        if (cursor != null) {
            return;
        }
        ChangeStreamIterable<Document> iter = iterable;
        if (!pipeline.isEmpty()) {
            iter = iterable.pipeline(pipeline);
        }
        if (resumeAfter != null) {
            iter = iter.resumeAfter(resumeAfter);
        }
        if (startAfter != null) {
            iter = iter.startAfter(startAfter);
        }
        if (startAtOperationTime != null) {
            iter = iter.startAtOperationTime(startAtOperationTime);
        }
        if (maxAwaitTimeMs >= 0) {
            iter = iter.maxAwaitTime(maxAwaitTimeMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
        cursor = iter.cursor();
    }
}
