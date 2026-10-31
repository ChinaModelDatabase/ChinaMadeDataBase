package com.mdb.driver;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * MDB Query Profiler for monitoring slow queries.
 *
 * <p>The profiler collects diagnostic information about query execution
 * to help identify performance bottlenecks.</p>
 *
 * <pre>{@code
 * MdbProfiler profiler = database.getProfiler();
 * profiler.enable(100); // log queries slower than 100ms
 *
 * List<Document> slowQueries = profiler.getSlowQueries();
 * for (Document query : slowQueries) {
 *     System.out.println("Slow query: " + query);
 * }
 *
 * profiler.disable();
 * }</pre>
 */
public class MdbProfiler {

    private final MongoDatabase database;

    /**
     * Creates an MdbProfiler for the given database.
     *
     * @param database the MDB database
     */
    MdbProfiler(MongoDatabase database) {
        this.database = database;
    }

    /**
     * Enables the profiler for this database.
     *
     * @param slowMs the threshold in milliseconds; queries slower than this
     *               are logged
     */
    public void enable(int slowMs) {
        Document command = new Document("profile", 1)
                .append("slowms", slowMs);
        database.runCommand(command);
    }

    /**
     * Disables the profiler for this database.
     */
    public void disable() {
        Document command = new Document("profile", 0);
        database.runCommand(command);
    }

    /**
     * Returns a list of slow queries from the system.profile collection.
     *
     * @return a list of Documents representing slow queries
     */
    public List<Document> getSlowQueries() {
        List<Document> queries = new ArrayList<>();
        for (Document doc : database.getCollection("system.profile").find()
                .sort(new Document("ts", -1))
                .limit(100)) {
            queries.add(doc);
        }
        return queries;
    }

    /**
     * Returns profiling statistics for this database.
     *
     * @return a Document containing profiler stats
     */
    public Document getStats() {
        return database.runCommand(new Document("profile", 2));
    }
}
