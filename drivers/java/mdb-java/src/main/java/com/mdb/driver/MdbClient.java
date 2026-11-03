package com.mdb.driver;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ConnectionPoolSettings;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Main entry point for connecting to an MDB (Xinchuang Document Database) server.
 *
 * <p>MDB is wire-protocol compatible with MongoDB 7.0. This client wraps the official
 * MongoDB Java Driver with MDB-specific extensions.</p>
 *
 * <pre>{@code
 * // Basic connection
 * MdbClient client = MdbClient.create("localhost", 27017);
 *
 * // Connection via URI
 * MdbClient client = MdbClient.create("mongodb://localhost:27017");
 *
 * // With authentication
 * MdbClient client = MdbClient.createWithAuth("localhost", 27017,
 *     "admin", "password123", "admin");
 *
 * MdbDatabase db = client.getDatabase("mydb");
 * }</pre>
 */
public class MdbClient implements AutoCloseable {

    private final MongoClient mongoClient;
    private final MongoClientSettings settings;

    /**
     * Creates an MdbClient wrapping the given MongoClient.
     *
     * @param mongoClient the underlying MongoDB client
     * @param settings    the client settings
     */
    private MdbClient(MongoClient mongoClient, MongoClientSettings settings) {
        this.mongoClient = mongoClient;
        this.settings = settings;
    }

    /**
     * Creates a new MdbClient connected to the specified host and port.
     *
     * @param host the hostname or IP address of the MDB server
     * @param port the port number of the MDB server
     * @return a new MdbClient instance
     */
    public static MdbClient create(String host, int port) {
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://" + host + ":" + port))
                .applyToConnectionPoolSettings(connBuilder -> connBuilder
                        .maxSize(100)
                        .minSize(5)
                        .maxWaitTime(5000, TimeUnit.MILLISECONDS)
                        .maxConnectionIdleTime(60000, TimeUnit.MILLISECONDS))
                .applyToSocketSettings(socketBuilder -> socketBuilder
                        .connectTimeout(10000, TimeUnit.MILLISECONDS)
                        .readTimeout(30000, TimeUnit.MILLISECONDS));

        MongoClientSettings settings = builder.build();
        MongoClient client = MongoClients.create(settings);
        return new MdbClient(client, settings);
    }

    /**
     * Creates a new MdbClient connected via a connection URI string.
     *
     * @param uri the MongoDB connection URI (e.g., {@code mongodb://localhost:27017})
     * @return a new MdbClient instance
     */
    public static MdbClient create(String uri) {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .build();
        MongoClient client = MongoClients.create(settings);
        return new MdbClient(client, settings);
    }

    /**
     * Creates a new MdbClient with authentication credentials.
     *
     * @param host     the hostname or IP address
     * @param port     the port number
     * @param username the authentication username
     * @param password the authentication password
     * @param authDb   the authentication database name
     * @return a new MdbClient instance
     */
    public static MdbClient createWithAuth(String host, int port, String username, String password, String authDb) {
        String encodedPassword = java.net.URLEncoder.encode(password, java.nio.charset.StandardCharsets.UTF_8);
        String uri = String.format("mongodb://%s:%s@%s:%d/%s?authSource=%s",
                username, encodedPassword, host, port, authDb, authDb);

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .applyToConnectionPoolSettings(connBuilder -> connBuilder
                        .maxSize(100)
                        .minSize(5))
                .applyToSocketSettings(socketBuilder -> socketBuilder
                        .connectTimeout(10000, java.util.concurrent.TimeUnit.MILLISECONDS)
                        .readTimeout(30000, java.util.concurrent.TimeUnit.MILLISECONDS))
                .build();

        MongoClient client = MongoClients.create(settings);
        return new MdbClient(client, settings);
    }

    /**
     * Returns a wrapper for the specified database.
     *
     * @param databaseName the database name
     * @return an MdbDatabase instance
     */
    public MdbDatabase getDatabase(String databaseName) {
        MongoDatabase db = mongoClient.getDatabase(databaseName);
        return new MdbDatabase(this, db);
    }

    /**
     * Returns a list of all database names on the server.
     *
     * @return a list of database names
     */
    public List<String> listDatabases() {
        List<String> names = new ArrayList<>();
        for (String name : mongoClient.listDatabaseNames()) {
            names.add(name);
        }
        return names;
    }

    /**
     * Returns server status information.
     *
     * @return a Document containing server status
     */
    public Document getServerStatus() {
        return mongoClient.getDatabase("admin").runCommand(new Document("serverStatus", 1));
    }

    /**
     * Returns replica set status information. Throws if the server is not
     * part of a replica set.
     *
     * @return a Document containing replica set status
     * @throws com.mongodb.MongoCommandException if not in a replica set
     */
    public Document getReplicaSetStatus() {
        return mongoClient.getDatabase("admin").runCommand(new Document("replSetGetStatus", 1));
    }

    /**
     * Checks whether the client is currently connected to the server.
     *
     * @return true if connected
     */
    public boolean isConnected() {
        try {
            mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Returns the underlying MongoDB MongoClient.
     *
     * @return the wrapped MongoClient
     */
    public MongoClient getUnderlyingClient() {
        return mongoClient;
    }

    /**
     * Closes the client and releases all underlying resources.
     */
    @Override
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
