package mdb.driver;

import java.io.IOException;
import java.util.*;

public class GridFS {

    private static final int DEFAULT_CHUNK_SIZE = 255 * 1024;

    private final MdbDatabase db;
    private final String bucketName;
    private final int chunkSizeBytes;

    public GridFS(MdbDatabase db, String bucketName) {
        this.db = db;
        this.bucketName = bucketName;
        this.chunkSizeBytes = DEFAULT_CHUNK_SIZE;
    }

    public GridFS(MdbDatabase db, String bucketName, int chunkSizeBytes) {
        this.db = db;
        this.bucketName = bucketName;
        this.chunkSizeBytes = chunkSizeBytes;
    }

    public String upload(String filename, byte[] data) throws IOException {
        return upload(filename, data, null);
    }

    public String upload(String filename, byte[] data, String contentType) throws IOException {
        Bson.ObjectIdHolder fileId = Bson.oid();
        int totalChunks = (int) Math.ceil((double) data.length / chunkSizeBytes);

        MdbCollection filesColl = db.getCollection(bucketName + ".files");
        MdbCollection chunksColl = db.getCollection(bucketName + ".chunks");

        Map<String, Object> filesDoc = new LinkedHashMap<>();
        filesDoc.put("_id", fileId);
        filesDoc.put("filename", filename);
        filesDoc.put("length", (long) data.length);
        filesDoc.put("chunkSize", chunkSizeBytes);
        filesDoc.put("uploadDate", new Date());
        if (contentType != null) {
            filesDoc.put("contentType", contentType);
        }

        Map<String, Object> metadata = new LinkedHashMap<>();
        filesDoc.put("metadata", metadata);

        filesColl.insertOne(filesDoc);

        List<Map<String, Object>> chunks = new ArrayList<>();
        for (int i = 0; i < totalChunks; i++) {
            int offset = i * chunkSizeBytes;
            int len = Math.min(chunkSizeBytes, data.length - offset);
            byte[] chunkData = new byte[len];
            System.arraycopy(data, offset, chunkData, 0, len);

            Map<String, Object> chunk = new LinkedHashMap<>();
            chunk.put("_id", Bson.oid());
            chunk.put("files_id", fileId);
            chunk.put("n", i);
            chunk.put("data", chunkData);
            chunks.add(chunk);

            if (chunks.size() >= 100 || i == totalChunks - 1) {
                chunksColl.insertMany(chunks);
                chunks.clear();
            }
        }

        return fileId.toHexString();
    }

    public byte[] download(String fileId) throws IOException {
        byte[] idBytes = ObjectIds.fromHexString(fileId);
        Bson.ObjectIdHolder id = new Bson.ObjectIdHolder(idBytes);

        MdbCollection filesColl = db.getCollection(bucketName + ".files");
        Map<String, Object> filter = new LinkedHashMap<>();
        filter.put("_id", id);
        Map<String, Object> filesDoc = filesColl.findOne(filter);

        if (filesDoc == null) {
            throw new IOException("File not found: " + fileId);
        }

        long length = ((Number) filesDoc.get("length")).longValue();
        byte[] result = new byte[(int) length];

        MdbCollection chunksColl = db.getCollection(bucketName + ".chunks");
        FindCursor cursor = chunksColl.find(new LinkedHashMap<String, Object>() {{
            put("files_id", id);
        }}).sort(new LinkedHashMap<String, Object>() {{
            put("n", 1);
        }});

        int offset = 0;
        try {
            while (cursor.hasNext()) {
                Map<String, Object> chunk = cursor.next();
                byte[] chunkData = (byte[]) chunk.get("data");
                int len = Math.min(chunkData.length, result.length - offset);
                System.arraycopy(chunkData, 0, result, offset, len);
                offset += len;
            }
        } finally {
            cursor.close();
        }

        return result;
    }

    public List<Map<String, Object>> listFiles() throws IOException {
        MdbCollection filesColl = db.getCollection(bucketName + ".files");
        FindCursor cursor = filesColl.find(new LinkedHashMap<>());
        try {
            return cursor.toArray();
        } finally {
            cursor.close();
        }
    }

    public void delete(String fileId) throws IOException {
        byte[] idBytes = ObjectIds.fromHexString(fileId);
        Bson.ObjectIdHolder id = new Bson.ObjectIdHolder(idBytes);

        MdbCollection chunksColl = db.getCollection(bucketName + ".chunks");
        Map<String, Object> chunkFilter = new LinkedHashMap<>();
        chunkFilter.put("files_id", id);
        chunksColl.deleteMany(chunkFilter);

        MdbCollection filesColl = db.getCollection(bucketName + ".files");
        Map<String, Object> filesFilter = new LinkedHashMap<>();
        filesFilter.put("_id", id);
        filesColl.deleteOne(filesFilter);
    }

    public void drop() throws IOException {
        db.dropCollection(bucketName + ".files");
        db.dropCollection(bucketName + ".chunks");
    }
}
