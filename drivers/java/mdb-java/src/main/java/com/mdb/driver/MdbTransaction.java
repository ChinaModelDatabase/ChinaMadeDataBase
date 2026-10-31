package com.mdb.driver;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 * MDB Transaction wrapper for multi-document ACID transactions.
 *
 * <p>Provides a simplified API for managing transactions across collections.</p>
 *
 * <pre>{@code
 * try (MdbTransaction txn = client.startTransaction()) {
 *     MdbCollection users = txn.getCollection("users");
 *     users.insertOne(new Document("name", "Alice"));
 *
 *     MdbCollection orders = txn.getCollection("orders");
 *     orders.insertOne(new Document("userId", "alice").append("amount", 100));
 *
 *     txn.commitTransaction();
 * } catch (Exception e) {
 *     // transaction auto-aborts on close if not committed
 * }
 * }</pre>
 */
public class MdbTransaction implements AutoCloseable {

    private final ClientSession session;
    private final MdbClient mdbClient;
    private boolean committed = false;

    /**
     * Creates an MdbTransaction.
     *
     * @param session  the MongoDB client session
     * @param mdbClient the MDB client
     */
    MdbTransaction(ClientSession session, MdbClient mdbClient) {
        this.session = session;
        this.mdbClient = mdbClient;
    }

    /**
     * Begins the transaction on the current session.
     */
    public void startTransaction() {
        session.startTransaction();
    }

    /**
     * Commits the current transaction.
     *
     * @throws com.mongodb.MongoException if the commit fails
     */
    public void commitTransaction() {
        session.commitTransaction();
        committed = true;
    }

    /**
     * Aborts the current transaction.
     */
    public void abortTransaction() {
        if (!committed) {
            session.abortTransaction();
        }
    }

    /**
     * Returns a collection within the transaction context.
     *
     * @param databaseName the database name
     * @param collectionName the collection name
     * @return an MdbCollection bound to this transaction
     */
    public MdbCollection getCollection(String databaseName, String collectionName) {
        MongoDatabase db = mdbClient.getUnderlyingClient().getDatabase(databaseName);
        MongoCollection<Document> mongoCollection = db.getCollection(collectionName);
        MdbCollection mdbCollection = new MdbCollection(mongoCollection, session);
        mdbCollection.setDatabase(db);
        return mdbCollection;
    }

    /**
     * Returns the underlying ClientSession.
     *
     * @return the MongoDB ClientSession
     */
    public ClientSession getSession() {
        return session;
    }

    /**
     * Closes the transaction. If the transaction was not committed or aborted,
     * it is automatically aborted.
     */
    @Override
    public void close() {
        if (!committed) {
            try {
                session.abortTransaction();
            } catch (Exception ignored) {
            }
        }
        session.close();
    }
}
