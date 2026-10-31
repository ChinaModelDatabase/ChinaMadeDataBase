package mdb.driver;

import org.junit.Test;
import org.junit.Assume;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.*;

public class MdbNativeTest {

    // ==================== CRC32C Tests ====================

    @Test
    public void testCrc32cEmpty() {
        int crc = CRC32C.compute(new byte[0]);
        assertEquals(0x00000000, crc);
    }

    @Test
    public void testCrc32cSingleByte() {
        byte[] data = {0x00};
        int crc = CRC32C.compute(data);
        assertNotEquals(0, crc);
    }

    @Test
    public void testCrc32cKnownVector() {
        byte[] data = "123456789".getBytes();
        int crc = CRC32C.compute(data);
        assertEquals(0xE3069283, crc);
    }

    @Test
    public void testCrc32cBytes() {
        byte[] data = "123456789".getBytes();
        byte[] crcBytes = CRC32C.computeBytes(data);
        assertEquals(4, crcBytes.length);
        assertEquals((byte) 0x83, crcBytes[0]);
        assertEquals((byte) 0x92, crcBytes[1]);
        assertEquals((byte) 0x06, crcBytes[2]);
        assertEquals((byte) 0xE3, crcBytes[3]);
    }

    // ==================== ObjectId Tests ====================

    @Test
    public void testObjectIdGeneration() {
        byte[] id = ObjectIds.generate();
        assertEquals(12, id.length);
    }

    @Test
    public void testObjectIdHexRoundtrip() {
        byte[] id = ObjectIds.generate();
        String hex = ObjectIds.toHexString(id);
        assertEquals(24, hex.length());
        byte[] decoded = ObjectIds.fromHexString(hex);
        assertArrayEquals(id, decoded);
    }

    @Test
    public void testObjectIdTimestamp() {
        int before = (int) (System.currentTimeMillis() / 1000);
        byte[] id = ObjectIds.generate();
        int after = (int) (System.currentTimeMillis() / 1000);
        int ts = ObjectIds.getTimestamp(id);
        assertTrue(ts >= before);
        assertTrue(ts <= after);
    }

    @Test
    public void testObjectIdHolder() {
        Bson.ObjectIdHolder oid = Bson.oid();
        assertNotNull(oid.toHexString());
        assertEquals(24, oid.toHexString().length());
    }

    // ==================== BSON Tests ====================

    @Test
    public void testBsonEmptyDocument() {
        Map<String, Object> doc = new LinkedHashMap<>();
        byte[] bytes = Bson.encode(doc);
        assertEquals(5, bytes.length);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertTrue(decoded.isEmpty());
    }

    @Test
    public void testBsonInt32() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("x", 42);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(42, ((Number) decoded.get("x")).intValue());
    }

    @Test
    public void testBsonInt64() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("x", 123456789012345L);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(123456789012345L, ((Number) decoded.get("x")).longValue());
    }

    @Test
    public void testBsonDouble() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("x", 3.14159);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(3.14159, (Double) decoded.get("x"), 0.000001);
    }

    @Test
    public void testBsonString() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("name", "hello world");
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals("hello world", decoded.get("name"));
    }

    @Test
    public void testBsonBoolean() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("flag", true);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(true, decoded.get("flag"));
    }

    @Test
    public void testBsonBooleanFalse() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("flag", false);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(false, decoded.get("flag"));
    }

    @Test
    public void testBsonNull() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("nothing", null);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertNull(decoded.get("nothing"));
    }

    @Test
    public void testBsonObjectId() {
        Map<String, Object> doc = new LinkedHashMap<>();
        Bson.ObjectIdHolder oid = Bson.oid();
        doc.put("_id", oid);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(oid, decoded.get("_id"));
    }

    @Test
    public void testBsonDateTime() {
        Map<String, Object> doc = new LinkedHashMap<>();
        Date now = new Date();
        doc.put("ts", now);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(now.getTime(), ((Date) decoded.get("ts")).getTime());
    }

    @Test
    public void testBsonBinary() {
        Map<String, Object> doc = new LinkedHashMap<>();
        byte[] data = {1, 2, 3, 4, 5};
        doc.put("data", data);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertArrayEquals(data, (byte[]) decoded.get("data"));
    }

    @Test
    public void testBsonNestedDocument() {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("a", 1);
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("nested", inner);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        Map<String, Object> decodedInner = (Map<String, Object>) decoded.get("nested");
        assertEquals(1, ((Number) decodedInner.get("a")).intValue());
    }

    @Test
    public void testBsonArray() {
        Map<String, Object> doc = new LinkedHashMap<>();
        List<Object> arr = Arrays.asList(1, "two", 3.0, true, null);
        doc.put("arr", arr);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        List<Object> decodedArr = (List<Object>) decoded.get("arr");
        assertEquals(5, decodedArr.size());
        assertEquals(1, ((Number) decodedArr.get(0)).intValue());
        assertEquals("two", decodedArr.get(1));
        assertEquals(3.0, (Double) decodedArr.get(2), 0.001);
        assertEquals(true, decodedArr.get(3));
        assertNull(decodedArr.get(4));
    }

    @Test
    public void testBsonComplexRoundtrip() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("string", "hello");
        doc.put("int32", 42);
        doc.put("int64", 9999999999L);
        doc.put("double", 2.71828);
        doc.put("bool", true);
        doc.put("null", null);
        doc.put("oid", Bson.oid());
        doc.put("date", new Date());
        doc.put("binary", new byte[]{10, 20, 30});

        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("inner", "value");
        nested.put("num", 123);
        doc.put("nested", nested);

        List<Object> arr = Arrays.asList(1, "two", 3.0);
        doc.put("array", arr);

        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);

        assertEquals("hello", decoded.get("string"));
        assertEquals(42, ((Number) decoded.get("int32")).intValue());
        assertEquals(9999999999L, ((Number) decoded.get("int64")).longValue());
        assertEquals(2.71828, (Double) decoded.get("double"), 0.00001);
        assertEquals(true, decoded.get("bool"));
        assertNull(decoded.get("null"));
        assertNotNull(decoded.get("oid"));
        assertNotNull(decoded.get("date"));
        assertArrayEquals(new byte[]{10, 20, 30}, (byte[]) decoded.get("binary"));

        Map<String, Object> decodedNested = (Map<String, Object>) decoded.get("nested");
        assertEquals("value", decodedNested.get("inner"));
        assertEquals(123, ((Number) decodedNested.get("num")).intValue());

        List<Object> decodedArr = (List<Object>) decoded.get("array");
        assertEquals(3, decodedArr.size());
    }

    @Test
    public void testBsonUnicodeString() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("text", "\u00e9\u00e8\u00ea \u4f60\u597d \ud83d\ude00");
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals("\u00e9\u00e8\u00ea \u4f60\u597d \ud83d\ude00", decoded.get("text"));
    }

    @Test
    public void testBsonEmptyString() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("empty", "");
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals("", decoded.get("empty"));
    }

    @Test
    public void testBsonLargeDocument() {
        Map<String, Object> doc = new LinkedHashMap<>();
        for (int i = 0; i < 1000; i++) {
            doc.put("field_" + i, "value_" + i);
        }
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(1000, decoded.size());
        assertEquals("value_0", decoded.get("field_0"));
        assertEquals("value_999", decoded.get("field_999"));
    }

    @Test
    public void testBsonNegativeInt32() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("neg", -42);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(-42, ((Number) decoded.get("neg")).intValue());
    }

    @Test
    public void testBsonZeroInt32() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("zero", 0);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(0, ((Number) decoded.get("zero")).intValue());
    }

    @Test
    public void testBsonMaxMinInt32() {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("max", Integer.MAX_VALUE);
        doc.put("min", Integer.MIN_VALUE);
        byte[] bytes = Bson.encode(doc);
        Map<String, Object> decoded = Bson.decode(bytes);
        assertEquals(Integer.MAX_VALUE, ((Number) decoded.get("max")).intValue());
        assertEquals(Integer.MIN_VALUE, ((Number) decoded.get("min")).intValue());
    }

    // ==================== Wire Protocol Tests ====================

    @Test
    public void testWireProtocolBuildRequest() {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("ping", 1);
        byte[] msg = WireProtocol.buildRequestSimple(1, cmd);
        assertNotNull(msg);
        assertTrue(msg.length >= 16);

        int[] offset = {0};
        int msgLen = Bson.readInt32(msg, offset);
        assertEquals(msg.length, msgLen);

        int reqId = Bson.readInt32(msg, offset);
        assertEquals(1, reqId);

        int respTo = Bson.readInt32(msg, offset);
        assertEquals(0, respTo);

        int opCode = Bson.readInt32(msg, offset);
        assertEquals(WireProtocol.OP_MSG, opCode);
    }

    @Test
    public void testWireProtocolRoundtrip() {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("test", "value");
        cmd.put("number", 42);

        byte[] msg = WireProtocol.buildRequest(999, cmd, null, null);

        int[] offset = {0};
        Bson.readInt32(msg, offset);
        int reqId = Bson.readInt32(msg, offset);
        Bson.readInt32(msg, offset);
        Bson.readInt32(msg, offset);
        int flagBits = Bson.readInt32(msg, offset);

        assertEquals(999, reqId);
        assertEquals(0, flagBits);
    }

    @Test
    public void testWireProtocolWithDocuments() {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("insert", "test");

        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("name", "test");
        doc.put("value", 42);
        docs.add(doc);

        byte[] msg = WireProtocol.buildRequest(1, cmd, docs, "documents");
        assertNotNull(msg);
        assertTrue(msg.length > 16);
    }

    @Test
    public void testWireProtocolParseResponse() {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("ok", 1.0);

        byte[] cmdBytes = Bson.encode(cmd);
        int sectionSize = 4 + cmdBytes.length;
        int flagBits = 0;
        int messageLength = 16 + 4 + 1 + sectionSize;

        byte[] response = new byte[messageLength];
        int[] offset = {0};

        Bson.writeInt32ToArray(response, offset, messageLength);
        Bson.writeInt32ToArray(response, offset, 12345);
        Bson.writeInt32ToArray(response, offset, 999);
        Bson.writeInt32ToArray(response, offset, WireProtocol.OP_MSG);
        Bson.writeInt32ToArray(response, offset, flagBits);
        response[offset[0]++] = 0;
        Bson.writeInt32ToArray(response, offset, sectionSize);
        System.arraycopy(cmdBytes, 0, response, offset[0], cmdBytes.length);

        WireProtocol.OpMsg opMsg = WireProtocol.parseResponse(response);
        assertEquals(12345, opMsg.requestId);
        assertEquals(999, opMsg.responseTo);
        assertNotNull(opMsg.body);
        assertEquals(1.0, ((Number) opMsg.body.get("ok")).doubleValue(), 0.001);
    }

    // ==================== Integration Tests (require MDB server) ====================

    private MdbClient createTestClient() {
        String host = System.getProperty("mdb.host", "localhost");
        int port = Integer.parseInt(System.getProperty("mdb.port", "27017"));
        return new MdbClient(host, port);
    }

    private boolean isServerAvailable() {
        try (MdbClient client = createTestClient()) {
            client.connect();
            return client.ping();
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    public void testPing() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            assertTrue(client.ping());
        }
    }

    @Test
    public void testListDatabases() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            List<String> dbs = client.listDatabases();
            assertNotNull(dbs);
            assertTrue(dbs.contains("admin"));
        }
    }

    @Test
    public void testServerStatus() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            Map<String, Object> status = client.getServerStatus();
            assertNotNull(status);
            assertEquals(1.0, ((Number) status.get("ok")).doubleValue(), 0.001);
        }
    }

    @Test
    public void testCreateAndDropCollection() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("test_coll");
            List<String> colls = db.listCollections();
            assertTrue(colls.contains("test_coll"));
            db.dropCollection("test_coll");
            colls = db.listCollections();
            assertFalse(colls.contains("test_coll"));
        }
    }

    @Test
    public void testInsertAndFind() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("crud_test");
            MdbCollection coll = db.getCollection("crud_test");

            try {
                Map<String, Object> doc = new LinkedHashMap<>();
                doc.put("name", "Alice");
                doc.put("age", 30);
                MdbCollection.InsertOneResult result = coll.insertOne(doc);
                assertTrue(result.acknowledged);
                assertNotNull(result.insertedId);

                Map<String, Object> found = coll.findOne(new LinkedHashMap<>());
                assertNotNull(found);
                assertEquals("Alice", found.get("name"));
                assertEquals(30, ((Number) found.get("age")).intValue());
            } finally {
                db.dropCollection("crud_test");
            }
        }
    }

    @Test
    public void testInsertMany() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("batch_test");
            MdbCollection coll = db.getCollection("batch_test");

            try {
                List<Map<String, Object>> docs = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    Map<String, Object> doc = new LinkedHashMap<>();
                    doc.put("index", i);
                    doc.put("name", "item_" + i);
                    docs.add(doc);
                }

                MdbCollection.InsertManyResult result = coll.insertMany(docs);
                assertEquals(10, result.insertedCount);
                assertEquals(10, result.insertedIds.size());

                long count = coll.countDocuments(new LinkedHashMap<>());
                assertEquals(10, count);
            } finally {
                db.dropCollection("batch_test");
            }
        }
    }

    @Test
    public void testUpdateOne() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("update_test");
            MdbCollection coll = db.getCollection("update_test");

            try {
                Map<String, Object> doc = new LinkedHashMap<>();
                doc.put("name", "Bob");
                doc.put("age", 25);
                coll.insertOne(doc);

                Map<String, Object> filter = new LinkedHashMap<>();
                filter.put("name", "Bob");

                Map<String, Object> update = new LinkedHashMap<>();
                Map<String, Object> set = new LinkedHashMap<>();
                set.put("age", 26);
                update.put("$set", set);

                MdbCollection.UpdateResult result = coll.updateOne(filter, update);
                assertEquals(1, result.matchedCount);
                assertEquals(1, result.modifiedCount);

                Map<String, Object> found = coll.findOne(filter);
                assertEquals(26, ((Number) found.get("age")).intValue());
            } finally {
                db.dropCollection("update_test");
            }
        }
    }

    @Test
    public void testDeleteOne() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("delete_test");
            MdbCollection coll = db.getCollection("delete_test");

            try {
                Map<String, Object> doc = new LinkedHashMap<>();
                doc.put("name", "Charlie");
                coll.insertOne(doc);

                Map<String, Object> filter = new LinkedHashMap<>();
                filter.put("name", "Charlie");

                MdbCollection.DeleteResult result = coll.deleteOne(filter);
                assertEquals(1, result.deletedCount);

                Map<String, Object> found = coll.findOne(filter);
                assertNull(found);
            } finally {
                db.dropCollection("delete_test");
            }
        }
    }

    @Test
    public void testFindCursor() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("cursor_test");
            MdbCollection coll = db.getCollection("cursor_test");

            try {
                for (int i = 0; i < 50; i++) {
                    Map<String, Object> doc = new LinkedHashMap<>();
                    doc.put("index", i);
                    coll.insertOne(doc);
                }

                FindCursor cursor = coll.find(new LinkedHashMap<>());
                List<Map<String, Object>> all = cursor.toArray();
                assertEquals(50, all.size());
                cursor.close();

                cursor = coll.find(new LinkedHashMap<>()).limit(10);
                all = cursor.toArray();
                assertEquals(10, all.size());
                cursor.close();
            } finally {
                db.dropCollection("cursor_test");
            }
        }
    }

    @Test
    public void testCountDocuments() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("count_test");
            MdbCollection coll = db.getCollection("count_test");

            try {
                for (int i = 0; i < 5; i++) {
                    Map<String, Object> doc = new LinkedHashMap<>();
                    doc.put("type", "even");
                    coll.insertOne(doc);
                }
                for (int i = 0; i < 3; i++) {
                    Map<String, Object> doc = new LinkedHashMap<>();
                    doc.put("type", "odd");
                    coll.insertOne(doc);
                }

                assertEquals(8, coll.countDocuments(new LinkedHashMap<>()));

                Map<String, Object> filter = new LinkedHashMap<>();
                filter.put("type", "even");
                assertEquals(5, coll.countDocuments(filter));
            } finally {
                db.dropCollection("count_test");
            }
        }
    }

    @Test
    public void testCreateAndListIndexes() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("index_test");
            MdbCollection coll = db.getCollection("index_test");

            try {
                Map<String, Object> keys = new LinkedHashMap<>();
                keys.put("name", 1);
                coll.createIndex(keys);

                List<Map<String, Object>> indexes = coll.listIndexes();
                assertTrue(indexes.size() >= 2);
            } finally {
                db.dropCollection("index_test");
            }
        }
    }

    @Test
    public void testDistinct() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("distinct_test");
            MdbCollection coll = db.getCollection("distinct_test");

            try {
                for (String color : Arrays.asList("red", "blue", "red", "green", "blue")) {
                    Map<String, Object> doc = new LinkedHashMap<>();
                    doc.put("color", color);
                    coll.insertOne(doc);
                }

                Object values = coll.distinct("color", new LinkedHashMap<>());
                assertNotNull(values);
                List<?> list = (List<?>) values;
                assertEquals(3, list.size());
            } finally {
                db.dropCollection("distinct_test");
            }
        }
    }

    @Test
    public void testAggregate() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("agg_test");
            MdbCollection coll = db.getCollection("agg_test");

            try {
                for (int i = 0; i < 10; i++) {
                    Map<String, Object> doc = new LinkedHashMap<>();
                    doc.put("category", i % 2 == 0 ? "A" : "B");
                    doc.put("value", i);
                    coll.insertOne(doc);
                }

                List<Map<String, Object>> pipeline = new ArrayList<>();
                Map<String, Object> group = new LinkedHashMap<>();
                Map<String, Object> groupId = new LinkedHashMap<>();
                groupId.put("_id", "$category");
                group.put("$group", groupId);
                pipeline.add(group);

                Map<String, Object> result = coll.aggregate(pipeline);
                assertNotNull(result);
            } finally {
                db.dropCollection("agg_test");
            }
        }
    }

    @Test
    public void testGetStats() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            Map<String, Object> stats = db.getStats();
            assertNotNull(stats);
            assertEquals(1.0, ((Number) stats.get("ok")).doubleValue(), 0.001);
        }
    }

    @Test
    public void testGridFS() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            GridFS gridFS = new GridFS(db, "test_files");

            try {
                byte[] testData = "Hello, GridFS!".getBytes();
                String fileId = gridFS.upload("test.txt", testData);
                assertNotNull(fileId);

                byte[] downloaded = gridFS.download(fileId);
                assertArrayEquals(testData, downloaded);

                List<Map<String, Object>> files = gridFS.listFiles();
                assertEquals(1, files.size());

                gridFS.delete(fileId);
                files = gridFS.listFiles();
                assertEquals(0, files.size());
            } finally {
                gridFS.drop();
            }
        }
    }

    @Test
    public void testGridFSChunking() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            GridFS gridFS = new GridFS(db, "test_chunks", 100);

            try {
                byte[] testData = new byte[350];
                new Random(42).nextBytes(testData);
                String fileId = gridFS.upload("large.bin", testData);

                byte[] downloaded = gridFS.download(fileId);
                assertArrayEquals(testData, downloaded);

                gridFS.delete(fileId);
            } finally {
                gridFS.drop();
            }
        }
    }

    @Test
    public void testUpdateMany() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("updatemany_test");
            MdbCollection coll = db.getCollection("updatemany_test");

            try {
                for (int i = 0; i < 5; i++) {
                    Map<String, Object> doc = new LinkedHashMap<>();
                    doc.put("status", "pending");
                    doc.put("value", i);
                    coll.insertOne(doc);
                }

                Map<String, Object> filter = new LinkedHashMap<>();
                filter.put("status", "pending");

                Map<String, Object> update = new LinkedHashMap<>();
                Map<String, Object> set = new LinkedHashMap<>();
                set.put("status", "done");
                update.put("$set", set);

                MdbCollection.UpdateResult result = coll.updateMany(filter, update);
                assertEquals(5, result.matchedCount);
                assertEquals(5, result.modifiedCount);
            } finally {
                db.dropCollection("updatemany_test");
            }
        }
    }

    @Test
    public void testDeleteMany() throws IOException {
        Assume.assumeTrue("MDB server not available", isServerAvailable());
        try (MdbClient client = createTestClient()) {
            client.connect();
            MdbDatabase db = client.getDatabase("test_native");
            db.createCollection("deletemany_test");
            MdbCollection coll = db.getCollection("deletemany_test");

            try {
                for (int i = 0; i < 10; i++) {
                    Map<String, Object> doc = new LinkedHashMap<>();
                    doc.put("group", "delete_me");
                    coll.insertOne(doc);
                }

                Map<String, Object> filter = new LinkedHashMap<>();
                filter.put("group", "delete_me");

                MdbCollection.DeleteResult result = coll.deleteMany(filter);
                assertEquals(10, result.deletedCount);

                long count = coll.countDocuments(new LinkedHashMap<>());
                assertEquals(0, count);
            } finally {
                db.dropCollection("deletemany_test");
            }
        }
    }
}
