package mdb.driver;

import java.io.IOException;
import java.util.*;

public class MdbCollection {

    private final MdbConnection connection;
    private final String databaseName;
    private final String collectionName;

    public MdbCollection(MdbConnection connection, String databaseName, String collectionName) {
        this.connection = connection;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    MdbConnection getConnection() {
        return connection;
    }

    private Map<String, Object> withDb(Map<String, Object> cmd) {
        Map<String, Object> cmdWithDb = new LinkedHashMap<>(cmd);
        cmdWithDb.put("$db", databaseName);
        return cmdWithDb;
    }

    public InsertOneResult insertOne(Map<String, Object> document) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("insert", collectionName);
        cmd.put("ordered", true);

        List<Map<String, Object>> docs = new ArrayList<>();
        Map<String, Object> docCopy = new LinkedHashMap<>(document);
        if (!docCopy.containsKey("_id")) {
            docCopy.put("_id", Bson.oid());
        }
        docs.add(docCopy);

        WireProtocol.OpMsg response = connection.sendCommand(withDb(cmd), docs, "documents");
        Map<String, Object> body = response.body;

        Object writeErrors = body.get("writeErrors");
        if (writeErrors != null && !((List<?>) writeErrors).isEmpty()) {
            Map<String, Object> error = (Map<String, Object>) ((List<?>) writeErrors).get(0);
            throw new IOException("Insert failed: " + error.get("errmsg"));
        }

        long insertedCount = 0;
        Object n = body.get("n");
        if (n != null) insertedCount = ((Number) n).longValue();

        Object insertedId = docCopy.get("_id");
        return new InsertOneResult(insertedCount > 0, insertedId);
    }

    public InsertManyResult insertMany(List<Map<String, Object>> documents) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("insert", collectionName);
        cmd.put("ordered", true);

        List<Map<String, Object>> docs = new ArrayList<>();
        List<Object> ids = new ArrayList<>();
        for (Map<String, Object> doc : documents) {
            Map<String, Object> docCopy = new LinkedHashMap<>(doc);
            if (!docCopy.containsKey("_id")) {
                docCopy.put("_id", Bson.oid());
            }
            ids.add(docCopy.get("_id"));
            docs.add(docCopy);
        }

        WireProtocol.OpMsg response = connection.sendCommand(withDb(cmd), docs, "documents");
        Map<String, Object> body = response.body;

        Object writeErrors = body.get("writeErrors");
        if (writeErrors != null && !((List<?>) writeErrors).isEmpty()) {
            Map<String, Object> error = (Map<String, Object>) ((List<?>) writeErrors).get(0);
            throw new IOException("Insert failed: " + error.get("errmsg"));
        }

        long insertedCount = 0;
        Object n = body.get("n");
        if (n != null) insertedCount = ((Number) n).longValue();

        return new InsertManyResult(insertedCount, ids);
    }

    public FindCursor find(Map<String, Object> filter) throws IOException {
        return new FindCursor(this, filter);
    }

    public Map<String, Object> findOne(Map<String, Object> filter) throws IOException {
        FindCursor cursor = find(filter).limit(1);
        try {
            if (cursor.hasNext()) {
                return cursor.next();
            }
            return null;
        } finally {
            cursor.close();
        }
    }

    public UpdateResult updateOne(Map<String, Object> filter, Map<String, Object> update) throws IOException {
        return doUpdate(filter, update, false);
    }

    public UpdateResult updateMany(Map<String, Object> filter, Map<String, Object> update) throws IOException {
        return doUpdate(filter, update, true);
    }

    private UpdateResult doUpdate(Map<String, Object> filter, Map<String, Object> update, boolean multi) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("update", collectionName);

        List<Map<String, Object>> updates = new ArrayList<>();
        Map<String, Object> updateDoc = new LinkedHashMap<>();
        updateDoc.put("q", filter);
        updateDoc.put("u", update);
        updateDoc.put("multi", multi);
        updateDoc.put("upsert", false);
        updates.add(updateDoc);

        WireProtocol.OpMsg response = connection.sendCommand(withDb(cmd), updates, "updates");
        Map<String, Object> body = response.body;

        long matched = 0, modified = 0;
        Object n = body.get("n");
        if (n != null) matched = ((Number) n).longValue();
        Object nModified = body.get("nModified");
        if (nModified != null) modified = ((Number) nModified).longValue();

        return new UpdateResult(matched, modified);
    }

    public DeleteResult deleteOne(Map<String, Object> filter) throws IOException {
        return doDelete(filter, false);
    }

    public DeleteResult deleteMany(Map<String, Object> filter) throws IOException {
        return doDelete(filter, true);
    }

    private DeleteResult doDelete(Map<String, Object> filter, boolean multi) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("delete", collectionName);

        List<Map<String, Object>> deletes = new ArrayList<>();
        Map<String, Object> deleteDoc = new LinkedHashMap<>();
        deleteDoc.put("q", filter);
        deleteDoc.put("limit", multi ? 0 : 1);
        deletes.add(deleteDoc);

        WireProtocol.OpMsg response = connection.sendCommand(withDb(cmd), deletes, "deletes");
        Map<String, Object> body = response.body;

        long deleted = 0;
        Object n = body.get("n");
        if (n != null) deleted = ((Number) n).longValue();

        return new DeleteResult(deleted);
    }

    public Map<String, Object> aggregate(List<Map<String, Object>> pipeline) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("aggregate", collectionName);
        cmd.put("pipeline", pipeline);
        cmd.put("cursor", new LinkedHashMap<String, Object>() {{ put("batchSize", 1000); }});

        WireProtocol.OpMsg response = connection.sendCommand(withDb(cmd));
        return response.body;
    }

    public Object distinct(String key, Map<String, Object> filter) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("distinct", collectionName);
        cmd.put("key", key);
        if (filter != null && !filter.isEmpty()) {
            cmd.put("query", filter);
        }

        WireProtocol.OpMsg response = connection.sendCommand(withDb(cmd));
        return response.body.get("values");
    }

    public long countDocuments(Map<String, Object> filter) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("count", collectionName);
        if (filter != null && !filter.isEmpty()) {
            cmd.put("query", filter);
        }

        WireProtocol.OpMsg response = connection.sendCommand(withDb(cmd));
        Object n = response.body.get("n");
        if (n == null) n = response.body.get("ok");
        return n != null ? ((Number) n).longValue() : 0;
    }

    public void createIndex(Map<String, Object> keys) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("createIndexes", collectionName);

        List<Map<String, Object>> indexes = new ArrayList<>();
        Map<String, Object> index = new LinkedHashMap<>();
        StringBuilder nameBuilder = new StringBuilder();
        for (Map.Entry<String, Object> entry : keys.entrySet()) {
            nameBuilder.append(entry.getKey()).append("_").append(entry.getValue()).append("_");
        }
        index.put("key", keys);
        index.put("name", nameBuilder.toString());
        indexes.add(index);

        cmd.put("indexes", indexes);
        connection.sendCommand(withDb(cmd));
    }

    public void dropIndex(String name) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("dropIndexes", collectionName);
        cmd.put("index", name);
        connection.sendCommand(withDb(cmd));
    }

    public List<Map<String, Object>> listIndexes() throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("listIndexes", collectionName);

        WireProtocol.OpMsg response = connection.sendCommand(withDb(cmd));
        Map<String, Object> body = response.body;

        List<Map<String, Object>> indexes = new ArrayList<>();
        Object cursor = body.get("cursor");
        if (cursor instanceof Map) {
            List<Map<String, Object>> firstBatch = (List<Map<String, Object>>) ((Map<?, ?>) cursor).get("firstBatch");
            if (firstBatch != null) {
                indexes.addAll(firstBatch);
            }
        }
        return indexes;
    }

    public Map<String, Object> getStats() throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("collStats", collectionName);
        return connection.sendCommand(withDb(cmd)).body;
    }

    public Map<String, Object> explain(Map<String, Object> filter) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("explain", new LinkedHashMap<String, Object>() {{
            put("find", collectionName);
            put("filter", filter != null ? filter : new LinkedHashMap<>());
        }});
        return connection.sendCommand(withDb(cmd)).body;
    }

    WireProtocol.OpMsg findRaw(Map<String, Object> filter, Map<String, Object> sort,
                                int limit, int skip, Map<String, Object> projection) throws IOException {
        Map<String, Object> cmd = new LinkedHashMap<>();
        cmd.put("find", collectionName);
        cmd.put("filter", filter != null ? filter : new LinkedHashMap<>());
        if (sort != null && !sort.isEmpty()) cmd.put("sort", sort);
        if (limit != 0) cmd.put("limit", limit);
        if (skip != 0) cmd.put("skip", skip);
        if (projection != null && !projection.isEmpty()) cmd.put("projection", projection);

        return connection.sendCommand(withDb(cmd));
    }

    public static class InsertOneResult {
        public final boolean acknowledged;
        public final Object insertedId;

        public InsertOneResult(boolean acknowledged, Object insertedId) {
            this.acknowledged = acknowledged;
            this.insertedId = insertedId;
        }

        @Override
        public String toString() {
            return "InsertOneResult{acknowledged=" + acknowledged + ", insertedId=" + insertedId + '}';
        }
    }

    public static class InsertManyResult {
        public final long insertedCount;
        public final List<Object> insertedIds;

        public InsertManyResult(long insertedCount, List<Object> insertedIds) {
            this.insertedCount = insertedCount;
            this.insertedIds = insertedIds;
        }

        @Override
        public String toString() {
            return "InsertManyResult{insertedCount=" + insertedCount + ", insertedIds=" + insertedIds + '}';
        }
    }

    public static class UpdateResult {
        public final long matchedCount;
        public final long modifiedCount;

        public UpdateResult(long matchedCount, long modifiedCount) {
            this.matchedCount = matchedCount;
            this.modifiedCount = modifiedCount;
        }

        @Override
        public String toString() {
            return "UpdateResult{matchedCount=" + matchedCount + ", modifiedCount=" + modifiedCount + '}';
        }
    }

    public static class DeleteResult {
        public final long deletedCount;

        public DeleteResult(long deletedCount) {
            this.deletedCount = deletedCount;
        }

        @Override
        public String toString() {
            return "DeleteResult{deletedCount=" + deletedCount + '}';
        }
    }
}
