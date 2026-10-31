package mdb.driver;

import java.io.IOException;
import java.util.*;

public class FindCursor implements Iterator<Map<String, Object>>, AutoCloseable {

    private final MdbCollection collection;
    private final Map<String, Object> filter;
    private Map<String, Object> sort;
    private int limit = 0;
    private int skip = 0;
    private Map<String, Object> projection;

    private long cursorId = 0;
    private List<Map<String, Object>> currentBatch;
    private int batchIndex = 0;
    private boolean exhausted = false;
    private boolean initialized = false;

    FindCursor(MdbCollection collection, Map<String, Object> filter) {
        this.collection = collection;
        this.filter = filter != null ? filter : new LinkedHashMap<>();
        this.currentBatch = Collections.emptyList();
    }

    public FindCursor sort(Map<String, Object> sort) {
        this.sort = sort;
        return this;
    }

    public FindCursor limit(int n) {
        this.limit = n;
        return this;
    }

    public FindCursor skip(int n) {
        this.skip = n;
        return this;
    }

    public FindCursor projection(Map<String, Object> projection) {
        this.projection = projection;
        return this;
    }

    @Override
    public boolean hasNext() {
        if (batchIndex < currentBatch.size()) {
            return true;
        }
        if (exhausted) {
            return false;
        }
        try {
            fetchBatch();
        } catch (IOException e) {
            throw new RuntimeException("Failed to fetch cursor batch", e);
        }
        return batchIndex < currentBatch.size();
    }

    @Override
    public Map<String, Object> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Map<String, Object> doc = currentBatch.get(batchIndex++);
        if (batchIndex >= currentBatch.size() && cursorId != 0) {
            try {
                fetchBatch();
            } catch (IOException e) {
                throw new RuntimeException("Failed to fetch cursor batch", e);
            }
        }
        return doc;
    }

    private void fetchBatch() throws IOException {
        String dbName = collection.getDatabaseName();
        if (cursorId == 0 && !initialized) {
            initialized = true;
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("find", collection.getCollectionName());
            cmd.put("filter", filter);
            cmd.put("$db", dbName);
            if (sort != null && !sort.isEmpty()) cmd.put("sort", sort);
            if (limit != 0) cmd.put("limit", limit);
            if (skip != 0) cmd.put("skip", skip);
            if (projection != null && !projection.isEmpty()) cmd.put("projection", projection);

            Map<String, Object> cursorCmd = new LinkedHashMap<>();
            int batchSize = limit > 0 ? Math.min(limit, 101) : 101;
            cursorCmd.put("batchSize", batchSize);
            cmd.put("cursor", cursorCmd);

            WireProtocol.OpMsg response = collection.getConnection().sendCommand(cmd);
            parseCursorResponse(response);
        } else if (cursorId != 0) {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("getMore", cursorId);
            cmd.put("collection", collection.getCollectionName());
            cmd.put("$db", dbName);
            int batchSize = limit > 0 ? Math.min(limit - (int) countReturned(), 101) : 101;
            cmd.put("batchSize", batchSize);

            WireProtocol.OpMsg response = collection.getConnection().sendCommand(cmd);
            parseCursorResponse(response);
        }
    }

    private long countReturned() {
        return (long) batchIndex;
    }

    @SuppressWarnings("unchecked")
    private void parseCursorResponse(WireProtocol.OpMsg response) {
        Map<String, Object> body = response.body;
        Object cursorObj = body.get("cursor");
        if (cursorObj instanceof Map) {
            Map<String, Object> cursorMap = (Map<String, Object>) cursorObj;
            cursorId = cursorMap.get("id") != null ? ((Number) cursorMap.get("id")).longValue() : 0;
            currentBatch = (List<Map<String, Object>>) cursorMap.get("firstBatch");
            if (currentBatch == null) currentBatch = Collections.emptyList();
        } else {
            currentBatch = response.getDocuments();
            cursorId = 0;
        }
        batchIndex = 0;
        if (cursorId == 0 && currentBatch.isEmpty()) {
            exhausted = true;
        }
    }

    public List<Map<String, Object>> toArray() {
        List<Map<String, Object>> result = new ArrayList<>();
        while (hasNext()) {
            result.add(next());
        }
        return result;
    }

    public Map<String, Object> explain() throws IOException {
        return collection.explain(filter);
    }

    @Override
    public void close() {
        if (cursorId != 0) {
            try {
                Map<String, Object> cmd = new LinkedHashMap<>();
                cmd.put("killCursors", collection.getCollectionName());
                cmd.put("cursors", Arrays.asList(cursorId));
                cmd.put("$db", collection.getDatabaseName());
                collection.getConnection().sendCommand(cmd);
            } catch (IOException ignored) {
            }
            cursorId = 0;
        }
        exhausted = true;
    }

    private MdbConnection getConnection() {
        return collection.getConnection();
    }
}
