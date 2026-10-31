package mdb.example;

import mdb.driver.GridFS;
import mdb.driver.MdbClient;
import mdb.driver.MdbDatabase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * GridFS 示例：文件上传、下载、列表、删除，以及大文件分块存储。
 *
 * <p>GridFS 将大文件拆分为 chunk 存入两个集合：</p>
 * <ul>
 *   <li>{bucket}.files  —— 文件元数据（文件名、长度、chunkSize、uploadDate 等）</li>
 *   <li>{bucket}.chunks —— 文件数据块（files_id、序号 n、data 二进制）</li>
 * </ul>
 *
 * <p>默认 chunk 大小为 255KB；也可通过构造器自定义更小的 chunk 以演示分块逻辑。</p>
 */
public final class GridFSExample {

    private static final String DB_NAME = "mdb_example_db";
    private static final String BUCKET = "attachments";

    private GridFSExample() {
    }

    public static void run(String host, int port) throws Exception {
        try (MdbClient client = new MdbClient(host, port)) {
            client.connect();
            MdbDatabase db = client.getDatabase(DB_NAME);

            // 每次运行前清理 bucket
            GridFS gridFS = new GridFS(db, BUCKET);
            try {
                gridFS.drop();
            } catch (Exception ignored) {
            }

            // ------------------------------------------------------------------
            // 6.1 上传文件
            // ------------------------------------------------------------------
            System.out.println("--- 6.1 上传文件 ---");
            byte[] readme = ("MDB GridFS Demo\n"
                    + "这是一个用于演示 GridFS 上传/下载的示例文件。\n"
                    + "自研驱动会自动为文件生成 ObjectId，并写入 files 与 chunks 集合。\n")
                    .getBytes(StandardCharsets.UTF_8);

            // upload(filename, data, contentType)
            String fileId = gridFS.upload("readme.txt", readme, "text/plain");
            System.out.println("上传成功, fileId = " + fileId);
            System.out.println("文件大小 = " + readme.length + " bytes");

            // 查看 files 集合中的元数据
            List<Map<String, Object>> files = gridFS.listFiles();
            System.out.println("files 集合文档数 = " + files.size());
            for (Map<String, Object> f : files) {
                System.out.println("  - filename=" + f.get("filename")
                        + ", length=" + f.get("length")
                        + ", chunkSize=" + f.get("chunkSize")
                        + ", contentType=" + f.get("contentType")
                        + ", uploadDate=" + f.get("uploadDate"));
            }

            // ------------------------------------------------------------------
            // 6.2 下载文件
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 6.2 下载文件 ---");
            byte[] downloaded = gridFS.download(fileId);
            System.out.println("下载大小 = " + downloaded.length + " bytes");
            boolean identical = Arrays.equals(readme, downloaded);
            System.out.println("内容一致 = " + identical);
            System.out.println("下载内容预览:");
            System.out.println(new String(downloaded, StandardCharsets.UTF_8).trim());

            // ------------------------------------------------------------------
            // 6.3 列出所有文件
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 6.3 列出文件 ---");
            // 再上传两个文件，演示多文件列表
            gridFS.upload("config.json", "{\"env\":\"dev\",\"debug\":true}".getBytes(StandardCharsets.UTF_8), "application/json");
            gridFS.upload("notes.txt", "学习 MDB 自研驱动。".getBytes(StandardCharsets.UTF_8), "text/plain");

            files = gridFS.listFiles();
            System.out.println("当前 bucket 文件列表 (" + BUCKET + "):");
            for (Map<String, Object> f : files) {
                Object id = f.get("_id");
                String hex = id instanceof mdb.driver.Bson.ObjectIdHolder
                        ? ((mdb.driver.Bson.ObjectIdHolder) id).toHexString()
                        : String.valueOf(id);
                System.out.println("  - " + f.get("filename")
                        + "  id=" + hex
                        + "  size=" + f.get("length")
                        + "B");
            }

            // ------------------------------------------------------------------
            // 6.4 删除文件
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 6.4 删除文件 ---");
            // 删除 notes.txt：先从列表中找到其 fileId
            String notesId = null;
            for (Map<String, Object> f : files) {
                if ("notes.txt".equals(f.get("filename"))) {
                    Object id = f.get("_id");
                    notesId = id instanceof mdb.driver.Bson.ObjectIdHolder
                            ? ((mdb.driver.Bson.ObjectIdHolder) id).toHexString()
                            : String.valueOf(id);
                }
            }
            if (notesId != null) {
                gridFS.delete(notesId);
                System.out.println("已删除 notes.txt (id=" + notesId + ")");
                files = gridFS.listFiles();
                System.out.println("删除后剩余文件数 = " + files.size());
                for (Map<String, Object> f : files) {
                    System.out.println("  - " + f.get("filename"));
                }
            } else {
                System.out.println("未找到 notes.txt");
            }

            // ------------------------------------------------------------------
            // 6.5 大文件上传（分块存储）
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 6.5 大文件上传（分块存储） ---");
            // 使用较小的 chunk 大小（1KB），即使文件只有几 KB 也会被拆成多个 chunk
            // 生产环境默认 chunk 为 255KB
            int chunkSize = 1024;
            GridFS chunkedFS = new GridFS(db, "large_files", chunkSize);

            int fileSize = 8 * 1024; // 8KB
            byte[] largeData = new byte[fileSize];
            new Random(12345).nextBytes(largeData);
            // 写入可识别的文件头，便于校验
            byte[] header = "LARGE-FILE-HEADER\n".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(header, 0, largeData, 0, header.length);

            System.out.println("大文件大小 = " + fileSize + " bytes, chunkSize = " + chunkSize
                    + " -> 预计 chunk 数 = " + ((fileSize + chunkSize - 1) / chunkSize));

            String largeId = chunkedFS.upload("large.bin", largeData, "application/octet-stream");
            System.out.println("大文件上传成功, fileId = " + largeId);

            // 查看 chunks 集合（GridFS 使用 "{bucket}.chunks" 作为集合名）
            MdbCollectionAccess chunks = new MdbCollectionAccess(db, "large_files.chunks");
            List<Map<String, Object>> chunkDocs = chunks.find(new LinkedHashMap<>());
            System.out.println("chunks 集合文档数 = " + chunkDocs.size());
            for (Map<String, Object> c : chunkDocs) {
                byte[] data = (byte[]) c.get("data");
                System.out.println("  - n=" + c.get("n") + ", data length=" + (data == null ? 0 : data.length));
            }

            // 下载并校验
            byte[] largeDownloaded = chunkedFS.download(largeId);
            System.out.println("下载大小 = " + largeDownloaded.length + " bytes");
            System.out.println("内容一致 = " + Arrays.equals(largeData, largeDownloaded));
            System.out.println("文件头 = " + new String(largeDownloaded, 0, header.length, StandardCharsets.UTF_8).trim());

            // ------------------------------------------------------------------
            // 6.6 清理全部 GridFS 数据
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 6.6 清理 ---");
            gridFS.drop();
            chunkedFS.drop();
            System.out.println("bucket " + BUCKET + " 与 large_files 已清理");
        }
    }

    /**
     * 轻量封装：直接通过数据库句柄访问任意集合，
     * 用于查看 GridFS 内部的 chunks 集合。
     */
    private static final class MdbCollectionAccess {
        private final MdbDatabase db;
        private final String fullName;

        MdbCollectionAccess(MdbDatabase db, String fullName) {
            this.db = db;
            this.fullName = fullName;
        }

        List<Map<String, Object>> find(Map<String, Object> filter) throws Exception {
            // GridFS 内部集合名为 "{bucket}.chunks" / "{bucket}.files"，直接作为集合名使用
            mdb.driver.MdbCollection coll = db.getCollection(fullName);
            mdb.driver.FindCursor cursor = coll.find(filter);
            try {
                return cursor.toArray();
            } finally {
                cursor.close();
            }
        }
    }

    /** 独立运行入口：GridFSExample [host] [port] */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 27017;
        run(host, port);
    }
}
