package com.mdb.driver;

import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * MDB GridFS operations for storing and retrieving large files.
 *
 * <p>GridFS is a specification for storing and retrieving files that exceed the
 * BSON document size limit of 16 MB. Files are split into chunks and stored
 * across multiple documents.</p>
 *
 * <pre>{@code
 * MdbGridFSBucket gridFS = collection.getGridFSBucket();
 * ObjectId fileId = gridFS.uploadFromBytes("photo.jpg", imageBytes);
 * byte[] downloaded = gridFS.downloadToBytes(fileId);
 * }</pre>
 */
public class MdbGridFSBucket {

    private final GridFSBucket bucket;

    /**
     * Creates an MdbGridFSBucket wrapping the given GridFSBucket.
     *
     * @param bucket the underlying MongoDB GridFSBucket
     */
    MdbGridFSBucket(GridFSBucket bucket) {
        this.bucket = bucket;
    }

    /**
     * Creates an MdbGridFSBucket using the default bucket name ("fs").
     *
     * @param database the MDB database
     * @return a new MdbGridFSBucket
     */
    public static MdbGridFSBucket create(MdbDatabase database) {
        MongoDatabase mongoDb = database.getUnderlyingDatabase();
        GridFSBucket bucket = GridFSBuckets.create(mongoDb);
        return new MdbGridFSBucket(bucket);
    }

    /**
     * Creates an MdbGridFSBucket with a custom bucket name.
     *
     * @param database   the MDB database
     * @param bucketName the bucket name
     * @return a new MdbGridFSBucket
     */
    public static MdbGridFSBucket create(MdbDatabase database, String bucketName) {
        MongoDatabase mongoDb = database.getUnderlyingDatabase();
        GridFSBucket bucket = GridFSBuckets.create(mongoDb, bucketName);
        return new MdbGridFSBucket(bucket);
    }

    /**
     * Uploads data from a byte array.
     *
     * @param filename the name of the file
     * @param data     the file data
     * @return the ObjectId of the uploaded file
     */
    public ObjectId uploadFromBytes(String filename, byte[] data) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
        BsonObjectId id = bucket.uploadFromStream(filename, inputStream);
        return id.getValue();
    }

    /**
     * Uploads data from an InputStream.
     *
     * @param filename    the name of the file
     * @param inputStream the input stream containing the file data
     * @return the ObjectId of the uploaded file
     */
    public ObjectId uploadFromStream(String filename, InputStream inputStream) {
        BsonObjectId id = bucket.uploadFromStream(filename, inputStream);
        return id.getValue();
    }

    /**
     * Downloads a file to a byte array.
     *
     * @param fileId the ObjectId of the file to download
     * @return the file data as a byte array
     */
    public byte[] downloadToBytes(ObjectId fileId) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        bucket.downloadToStream(fileId, outputStream);
        return outputStream.toByteArray();
    }

    /**
     * Downloads a file to an OutputStream.
     *
     * @param fileId       the ObjectId of the file to download
     * @param outputStream the output stream to write to
     */
    public void downloadToStream(ObjectId fileId, OutputStream outputStream) {
        bucket.downloadToStream(fileId, outputStream);
    }

    /**
     * Finds files matching the given filter.
     *
     * @param filter the query filter
     * @return a list of matching GridFS files
     */
    public List<Document> find(Document filter) {
        GridFSFindIterable iterable = bucket.find(filter);
        List<Document> results = new ArrayList<>();
        for (org.bson.Document doc : iterable) {
            results.add(doc);
        }
        return results;
    }

    /**
     * Deletes a file by its ObjectId.
     *
     * @param fileId the ObjectId of the file to delete
     */
    public void delete(ObjectId fileId) {
        bucket.delete(fileId);
    }

    /**
     * Renames a file.
     *
     * @param fileId      the ObjectId of the file
     * @param newFilename the new filename
     */
    public void rename(ObjectId fileId, String newFilename) {
        bucket.rename(fileId, newFilename);
    }

    /**
     * Returns the underlying MongoDB GridFSBucket.
     *
     * @return the wrapped GridFSBucket
     */
    public GridFSBucket getUnderlyingBucket() {
        return bucket;
    }
}
