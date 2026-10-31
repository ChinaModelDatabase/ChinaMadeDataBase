package com.mdb.driver;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.ListCollectionsIterable;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * MDB Database operations wrapper.
 *
 * <p>Provides access to collections and database-level operations on an MDB server.</p>
 *
 * <pre>{@code
 * MdbDatabase db = client.getDatabase("mydb");
 * MdbCollection col = db.getCollection("users");
 * db.createCollection("logs");
 * List<String> collections = db.listCollections();
 * }</pre>
 */
public class MdbDatabase {

    private final MdbClient mdbClient;
    private final MongoDatabase database;

    /**
     * Creates an MdbDatabase.
     *
     * @param mdbClient the parent MDB client
     * @param database  the underlying MongoDB database
     */
    MdbDatabase(MdbClient mdbClient, MongoDatabase database) {
        this.mdbClient = mdbClient;
        this.database = database;
    }

    /**
     * Returns the named collection wrapped as an MdbCollection.
     *
     * @param collectionName the collection name
     * @return an MdbCollection instance
     */
    public MdbCollection getCollection(String collectionName) {
        MongoCollection<Document> mongoCollection = database.getCollection(collectionName);
        MdbCollection mdbCollection = new MdbCollection(mongoCollection);
        mdbCollection.setDatabase(database);
        return mdbCollection;
    }

    /**
     * Returns a list of collection names in this database.
     *
     * @return list of collection names
     */
    public List<String> listCollections() {
        List<String> names = new ArrayList<>();
        ListCollectionsIterable<Document> collections = database.listCollections();
        for (Document doc : collections) {
            names.add(doc.getString("name"));
        }
        return names;
    }

    /**
     * Creates a collection with default options.
     *
     * @param collectionName the collection name
     */
    public void createCollection(String collectionName) {
        database.createCollection(collectionName);
    }

    /**
     * Creates a collection with the specified options.
     *
     * @param collectionName the collection name
     * @param options        creation options (e.g., capped, size, max)
     */
    public void createCollection(String collectionName, Document options) {
        database.createCollection(collectionName, new com.mongodb.client.model.CreateCollectionOptions()
                .capped(options.getBoolean("capped", false))
                .sizeInBytes(options.getLong("size", 0L))
                .maxDocuments(options.getLong("max", 0L)));
    }

    /**
     * Drops the named collection from this database.
     *
     * @param collectionName the collection name
     */
    public void dropCollection(String collectionName) {
        database.getCollection(collectionName).drop();
    }

    /**
     * Runs a database command.
     *
     * @param command the command document
     * @return the command result
     */
    public Document runCommand(Document command) {
        return database.runCommand(command);
    }

    /**
     * Returns database statistics.
     *
     * @return a Document containing database stats
     */
    public Document getStats() {
        return database.runCommand(new Document("dbStats", 1));
    }

    /**
     * Returns the profiler for this database.
     *
     * @return an MdbProfiler instance
     */
    public MdbProfiler getProfiler() {
        return new MdbProfiler(database);
    }

    /**
     * Creates a new user with the specified roles.
     *
     * @param username the username
     * @param password the password
     * @param roles    list of role documents (e.g., [{"role":"readWrite","db":"mydb"}])
     */
    public void createUser(String username, String password, List<Document> roles) {
        Document createUser = new Document("createUser", username)
                .append("pwd", password)
                .append("roles", roles);
        database.runCommand(createUser);
    }

    /**
     * Drops a user from this database.
     *
     * @param username the username to drop
     */
    public void dropUser(String username) {
        database.runCommand(new Document("dropUser", username));
    }

    /**
     * Returns a list of all users in this database.
     *
     * @return a list of user documents
     */
    public List<Document> getUsers() {
        Document result = database.runCommand(new Document("usersInfo", 1));
        List<Document> users = new ArrayList<>();
        Object userList = result.get("users");
        if (userList instanceof List) {
            for (Object obj : (List<?>) userList) {
                if (obj instanceof Document) {
                    users.add((Document) obj);
                }
            }
        }
        return users;
    }

    /**
     * Returns the database name.
     *
     * @return the database name
     */
    public String getName() {
        return database.getName();
    }

    /**
     * Returns the underlying MongoDB Database.
     *
     * @return the MongoDatabase
     */
    public MongoDatabase getUnderlyingDatabase() {
        return database;
    }
}
