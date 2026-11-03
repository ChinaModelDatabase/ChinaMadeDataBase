package com.mdb.driver;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Comprehensive tests for the MDB Java Driver.
 *
 * <p>These tests require a running MDB server on localhost:27017.
 * Tests are designed to be run independently and clean up after themselves.</p>
 */
public class MdbDriverTest {

    private MdbClient client;
    private MdbDatabase database;
    private static final String TEST_DB = "mdb_java_test";
    private static final String TEST_COLLECTION = "test_collection";

    @Before
    public void setUp() {
        try {
            client = MdbClient.create("localhost", 27017);
            database = client.getDatabase(TEST_DB);
            // Clean up test collection
            database.dropCollection(TEST_COLLECTION);
        } catch (Exception e) {
            fail("Failed to connect to MDB server: " + e.getMessage());
        }
    }

    @After
    public void tearDown() {
        if (client != null) {
            try {
                database.dropCollection(TEST_COLLECTION);
            } catch (Exception ignored) {
            }
            client.close();
        }
    }

    // ==================== MdbClient Tests ====================

    @Test
    public void testClientCreate() {
        assertNotNull("Client should not be null", client);
        assertTrue("Client should be connected", client.isConnected());
    }

    @Test
    public void testClientCreateWithUri() {
        MdbClient uriClient = MdbClient.create("mongodb://localhost:27017");
        assertNotNull("URI client should not be null", uriClient);
        assertTrue("URI client should be connected", uriClient.isConnected());
        uriClient.close();
    }

    @Test
    public void testListDatabases() {
        List<String> databases = client.listDatabases();
        assertNotNull("Database list should not be null", databases);
        assertTrue("Should have at least one database", databases.size() > 0);
        assertTrue("Should contain admin database", databases.contains("admin"));
    }

    @Test
    public void testServerStatus() {
        Document status = client.getServerStatus();
        assertNotNull("Server status should not be null", status);
        assertTrue("Server status should contain 'host'", status.containsKey("host"));
        assertTrue("Server status should contain 'version'", status.containsKey("version"));
    }

    @Test
    public void testGetDatabase() {
        MdbDatabase db = client.getDatabase(TEST_DB);
        assertNotNull("Database should not be null", db);
        assertEquals("Database name should match", TEST_DB, db.getName());
    }

    // ==================== MdbDatabase Tests ====================

    @Test
    public void testCreateCollection() {
        database.createCollection("test_create");
        List<String> collections = database.listCollections();
        assertTrue("Should contain created collection", collections.contains("test_create"));
        database.dropCollection("test_create");
    }

    @Test
    public void testCreateCollectionWithOptions() {
        Document options = new Document("capped", true).append("size", 1048576L);
        database.createCollection("test_capped", options);
        List<String> collections = database.listCollections();
        assertTrue("Should contain capped collection", collections.contains("test_capped"));
        database.dropCollection("test_capped");
    }

    @Test
    public void testDropCollection() {
        database.createCollection("test_drop");
        database.dropCollection("test_drop");
        List<String> collections = database.listCollections();
        assertFalse("Should not contain dropped collection", collections.contains("test_drop"));
    }

    @Test
    public void testListCollections() {
        database.createCollection("test_list_1");
        database.createCollection("test_list_2");
        List<String> collections = database.listCollections();
        assertTrue("Should contain test_list_1", collections.contains("test_list_1"));
        assertTrue("Should contain test_list_2", collections.contains("test_list_2"));
        database.dropCollection("test_list_1");
        database.dropCollection("test_list_2");
    }

    @Test
    public void testRunCommand() {
        Document result = database.runCommand(new Document("ping", 1));
        assertNotNull("Command result should not be null", result);
        assertEquals("Ping should return ok", 1.0, result.getDouble("ok").doubleValue());
    }

    @Test
    public void testGetStats() {
        Document stats = database.getStats();
        assertNotNull("Stats should not be null", stats);
        assertTrue("Stats should contain 'db'", stats.containsKey("db"));
    }

    @Test
    public void testGetProfiler() {
        MdbProfiler profiler = database.getProfiler();
        assertNotNull("Profiler should not be null", profiler);
        profiler.disable();
    }

    @Test
    public void testCreateAndDropUser() {
        List<Document> roles = Arrays.asList(
                new Document("role", "readWrite").append("db", TEST_DB)
        );
        try {
            database.createUser("test_user", "test_password123", roles);
            List<Document> users = database.getUsers();
            boolean found = false;
            for (Document user : users) {
                if ("test_user".equals(user.getString("user"))) {
                    found = true;
                    break;
                }
            }
            assertTrue("Should find created user", found);
            database.dropUser("test_user");
        } catch (Exception e) {
            // User creation may fail if auth is not configured
            System.out.println("User creation test skipped: " + e.getMessage());
        }
    }

    // ==================== MdbCollection Tests ====================

    @Test
    public void testInsertOne() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        Document doc = new Document("name", "Alice").append("age", 30);
        Object id = collection.insertOne(doc);
        assertNotNull("Inserted ID should not be null", id);
    }

    @Test
    public void testInsertMany() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        List<Document> docs = Arrays.asList(
                new Document("name", "Alice").append("age", 30),
                new Document("name", "Bob").append("age", 25),
                new Document("name", "Charlie").append("age", 35)
        );
        collection.insertMany(docs);
        assertEquals("Should have 3 documents", 3, collection.count());
    }

    @Test
    public void testFindOne() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        Document doc = new Document("name", "Alice").append("age", 30);
        collection.insertOne(doc);

        Document found = collection.findOne(new Document("name", "Alice"));
        assertNotNull("Found document should not be null", found);
        assertEquals("Name should match", "Alice", found.getString("name"));
        assertEquals("Age should match", 30, found.getInteger("age").intValue());
    }

    @Test
    public void testFindOneWithProjection() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        Document doc = new Document("name", "Alice").append("age", 30).append("email", "alice@example.com");
        collection.insertOne(doc);

        Document found = collection.findOne(
                new Document("name", "Alice"),
                new Document("name", 1).append("_id", 0)
        );
        assertNotNull("Found document should not be null", found);
        assertEquals("Name should match", "Alice", found.getString("name"));
        assertNull("Email should not be present", found.getString("email"));
    }

    @Test
    public void testFind() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("age", 30),
                new Document("name", "Bob").append("age", 25),
                new Document("name", "Charlie").append("age", 35)
        ));

        MdbCursor cursor = collection.find(new Document("age", new Document("$gte", 30)));
        int count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        cursor.close();
        assertEquals("Should find 2 documents with age >= 30", 2, count);
    }

    @Test
    public void testUpdateOne() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertOne(new Document("name", "Alice").append("age", 30));

        long modified = collection.updateOne(
                new Document("name", "Alice"),
                new Document("$set", new Document("age", 31))
        );
        assertEquals("Should modify 1 document", 1, modified);

        Document found = collection.findOne(new Document("name", "Alice"));
        assertEquals("Age should be updated", 31, found.getInteger("age").intValue());
    }

    @Test
    public void testUpdateMany() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("status", "active"),
                new Document("name", "Bob").append("status", "active"),
                new Document("name", "Charlie").append("status", "inactive")
        ));

        long modified = collection.updateMany(
                new Document("status", "active"),
                new Document("$set", new Document("verified", true))
        );
        assertEquals("Should modify 2 documents", 2, modified);
    }

    @Test
    public void testReplaceOne() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertOne(new Document("name", "Alice").append("age", 30));

        long replaced = collection.replaceOne(
                new Document("name", "Alice"),
                new Document("name", "Alice").append("age", 31).append("city", "Beijing")
        );
        assertEquals("Should replace 1 document", 1, replaced);

        Document found = collection.findOne(new Document("name", "Alice"));
        assertEquals("Age should be updated", 31, found.getInteger("age").intValue());
        assertEquals("City should be added", "Beijing", found.getString("city"));
    }

    @Test
    public void testDeleteOne() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertOne(new Document("name", "Alice"));

        long deleted = collection.deleteOne(new Document("name", "Alice"));
        assertEquals("Should delete 1 document", 1, deleted);
        assertEquals("Collection should be empty", 0, collection.count());
    }

    @Test
    public void testDeleteMany() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("status", "active"),
                new Document("name", "Bob").append("status", "active"),
                new Document("name", "Charlie").append("status", "inactive")
        ));

        long deleted = collection.deleteMany(new Document("status", "active"));
        assertEquals("Should delete 2 documents", 2, deleted);
        assertEquals("Should have 1 document remaining", 1, collection.count());
    }

    @Test
    public void testFindAndModify() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertOne(new Document("name", "Alice").append("counter", 0));

        Document result = collection.findAndModify(
                new Document("name", "Alice"),
                new Document("$inc", new Document("counter", 1)),
                new Document("returnDocument", "after")
        );
        assertNotNull("Result should not be null", result);
        assertEquals("Counter should be incremented", 1, result.getInteger("counter").intValue());
    }

    @Test
    public void testAggregate() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("department", "Engineering").append("salary", 100000),
                new Document("name", "Bob").append("department", "Engineering").append("salary", 120000),
                new Document("name", "Charlie").append("department", "Marketing").append("salary", 90000)
        ));

        List<Document> pipeline = Arrays.asList(
                new Document("$group", new Document("_id", "$department")
                        .append("totalSalary", new Document("$sum", "$salary"))
                        .append("count", new Document("$sum", 1)))
        );

        MdbCursor cursor = collection.aggregate(pipeline);
        int groupCount = 0;
        while (cursor.hasNext()) {
            Document group = cursor.next();
            assertNotNull("Group should have _id", group.get("_id"));
            groupCount++;
        }
        cursor.close();
        assertEquals("Should have 2 groups", 2, groupCount);
    }

    @Test
    public void testDistinct() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("city", "Beijing"),
                new Document("name", "Bob").append("city", "Shanghai"),
                new Document("name", "Charlie").append("city", "Beijing")
        ));

        List<String> cities = collection.distinct("city");
        assertEquals("Should have 2 distinct cities", 2, cities.size());
        assertTrue("Should contain Beijing", cities.contains("Beijing"));
        assertTrue("Should contain Shanghai", cities.contains("Shanghai"));
    }

    @Test
    public void testDistinctWithFilter() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("city", "Beijing").append("active", true),
                new Document("name", "Bob").append("city", "Shanghai").append("active", false),
                new Document("name", "Charlie").append("city", "Beijing").append("active", true)
        ));

        List<String> cities = collection.distinct("city", new Document("active", true));
        assertEquals("Should have 1 distinct city for active users", 1, cities.size());
        assertTrue("Should contain Beijing", cities.contains("Beijing"));
    }

    @Test
    public void testCount() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertMany(Arrays.asList(
                new Document("name", "Alice").append("status", "active"),
                new Document("name", "Bob").append("status", "inactive"),
                new Document("name", "Charlie").append("status", "active")
        ));

        assertEquals("Total count should be 3", 3, collection.count());
        assertEquals("Active count should be 2", 2, collection.count(new Document("status", "active")));
    }

    @Test
    public void testCreateAndDropIndex() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        String indexName = collection.createIndex(new Document("name", 1));
        assertNotNull("Index name should not be null", indexName);

        List<Document> indexes = collection.listIndexes();
        boolean found = false;
        for (Document index : indexes) {
            if ("name_1".equals(index.getString("name"))) {
                found = true;
                break;
            }
        }
        assertTrue("Should find created index", found);

        collection.dropIndex("name_1");
    }

    @Test
    public void testCreateIndexWithOptions() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        MdbIndexOptions options = new MdbIndexOptions()
                .unique(true)
                .sparse(true)
                .name("unique_name_idx");

        String indexName = collection.createIndex(new Document("name", 1), options.build());
        assertEquals("Index name should match", "unique_name_idx", indexName);

        collection.dropIndex("unique_name_idx");
    }

    @Test
    public void testCollectionStats() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertOne(new Document("test", "data"));

        Document stats = collection.getStats();
        assertNotNull("Stats should not be null", stats);
        assertTrue("Stats should contain 'ns'", stats.containsKey("ns"));
    }

    @Test
    public void testValidate() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        collection.insertOne(new Document("test", "data"));

        Document result = collection.validate();
        assertNotNull("Validate result should not be null", result);
    }

    @Test
    public void testWatch() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        MdbChangeStream stream = collection.watch();
        assertNotNull("Change stream should not be null", stream);
        stream.close();
    }

    @Test
    public void testWatchWithPipeline() {
        MdbCollection collection = database.getCollection(TEST_COLLECTION);
        List<Document> pipeline = Arrays.asList(
                new Document("$match", new Document("operationType", "insert"))
        );
        MdbChangeStream stream = collection.watch(pipeline);
        assertNotNull("Change stream with pipeline should not be null", stream);
        stream.close();
    }

    @Test
    public void testGridFSBucket() {
        MdbDatabase db = client.getDatabase(TEST_DB);
        MdbGridFSBucket gridFS = MdbGridFSBucket.create(db);
        assertNotNull("GridFS bucket should not be null", gridFS);

        // Upload
        byte[] data = "Hello MDB GridFS!".getBytes();
        ObjectId fileId = gridFS.uploadFromBytes("test.txt", data);
        assertNotNull("File ID should not be null", fileId);

        // Download
        byte[] downloaded = gridFS.downloadToBytes(fileId);
        assertArrayEquals("Downloaded data should match uploaded", data, downloaded);

        // Find
        List<Document> files = gridFS.find(new Document("filename", "test.txt"));
        assertEquals("Should find 1 file", 1, files.size());

        // Rename
        gridFS.rename(fileId, "renamed_test.txt");

        // Delete
        gridFS.delete(fileId);
    }

    // ==================== MdbIndexOptions Tests ====================

    @Test
    public void testMdbIndexOptionsBuilder() {
        MdbIndexOptions options = new MdbIndexOptions()
                .unique(true)
                .sparse(true)
                .expireAfterSeconds(3600)
                .name("test_index")
                .background(true);

        com.mongodb.client.model.IndexOptions built = options.build();
        assertNotNull("Built options should not be null", built);
        assertTrue("Should be unique", built.isUnique());
        assertTrue("Should be sparse", built.isSparse());
        assertEquals("Expire after seconds should match", Long.valueOf(3600L),
                built.getExpireAfter(java.util.concurrent.TimeUnit.SECONDS));
        assertEquals("Name should match", "test_index", built.getName());
        assertTrue("Should be background", built.isBackground());
    }

    // ==================== MdbWriteConcern Tests ====================

    @Test
    public void testMdbWriteConcern() {
        assertNotNull("ACKNOWLEDGED should not be null", MdbWriteConcern.ACKNOWLEDGED.toWriteConcern());
        assertNotNull("UNACKNOWLEDGED should not be null", MdbWriteConcern.UNACKNOWLEDGED.toWriteConcern());
        assertNotNull("MAJORITY should not be null", MdbWriteConcern.MAJORITY.toWriteConcern());
    }

    // ==================== MdbReadConcern Tests ====================

    @Test
    public void testMdbReadConcern() {
        assertNotNull("LOCAL should not be null", MdbReadConcern.LOCAL.toReadConcern());
        assertNotNull("MAJORITY should not be null", MdbReadConcern.MAJORITY.toReadConcern());
        assertNotNull("LINEARIZABLE should not be null", MdbReadConcern.LINEARIZABLE.toReadConcern());
        assertNotNull("SNAPSHOT should not be null", MdbReadConcern.SNAPSHOT.toReadConcern());
    }

    // ==================== MdbProfiler Tests ====================

    @Test
    public void testProfilerEnableDisable() {
        MdbProfiler profiler = database.getProfiler();
        try {
            profiler.enable(100);
            profiler.disable();
        } catch (Exception e) {
            // Profiler commands may fail on some configurations
            System.out.println("Profiler test skipped: " + e.getMessage());
        }
    }

    @Test
    public void testProfilerGetSlowQueries() {
        MdbProfiler profiler = database.getProfiler();
        List<Document> queries = profiler.getSlowQueries();
        assertNotNull("Slow queries list should not be null", queries);
    }

    @Test
    public void testProfilerGetStats() {
        MdbProfiler profiler = database.getProfiler();
        Document stats = profiler.getStats();
        assertNotNull("Profiler stats should not be null", stats);
    }
}
