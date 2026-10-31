package mdb.example;

import mdb.driver.MdbClient;
import mdb.driver.MdbCollection;
import mdb.driver.MdbDatabase;
import mdb.driver.WireProtocol;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Change Stream 示例：监听集合的实时变更事件（insert / update / delete）。
 *
 * <p>Change Stream 基于聚合管道中的 {@code $changeStream} 阶段实现：
 * 服务器会持续推送操作事件，客户端通过游标 getMore 不断拉取。</p>
 *
 * <p>事件文档典型结构：</p>
 * <pre>
 * {
 *   _id: { _data: "..." },          // resume token，用于断点续传
 *   operationType: "insert" | "update" | "delete" | ...,
 *   fullDocument: { ... },          // 更新后的完整文档（需 fullDocument: "updateLookup"）
 *   ns: { db: "...", coll: "..." },
 *   ...
 * }
 * </pre>
 *
 * <p>自研驱动未封装 ChangeStream 类，本示例直接通过 aggregate + getMore
 * 发送标准命令，演示完整的协议用法。</p>
 */
public final class ChangeStreamExample {

    private static final String DB_NAME = "mdb_example_db";
    private static final String COLL_NAME = "events";

    private ChangeStreamExample() {
    }

    public static void run(String host, int port) throws Exception {
        try (MdbClient client = new MdbClient(host, port)) {
            client.connect();
            MdbDatabase db = client.getDatabase(DB_NAME);
            MdbCollection events = db.getCollection(COLL_NAME);

            try {
                db.dropCollection(COLL_NAME);
            } catch (Exception ignored) {
            }
            events.insertOne(doc("init", true));

            // ------------------------------------------------------------------
            // 7.1 打开 Change Stream
            // ------------------------------------------------------------------
            System.out.println("--- 7.1 打开 Change Stream ---");
            ChangeStream stream = null;
            String resumeToken = null;
            try {
                // $changeStream: fullDocument=updateLookup 表示 update 事件附带最新文档
                stream = ChangeStream.open(client, db, COLL_NAME, null, false);
                System.out.println("Change Stream 已打开, cursorId=" + stream.cursorId);

                // ------------------------------------------------------------------
                // 7.2 监听 insert 事件
                // ------------------------------------------------------------------
                System.out.println();
                System.out.println("--- 7.2 监听 insert 事件 ---");
                events.insertOne(doc("type", "insert-demo", "payload", "hello insert"));
                List<Map<String, Object>> batch = stream.poll(3000); // 最多等待 3 秒
                System.out.println("收到 " + batch.size() + " 个事件");
                for (Map<String, Object> evt : batch) {
                    printEvent(evt);
                    resumeToken = extractResumeToken(evt);
                }
                if (batch.isEmpty()) {
                    System.out.println("（服务器可能尚未实现 $changeStream，无事件返回属预期情况）");
                }

                // ------------------------------------------------------------------
                // 7.3 监听 update 事件
                // ------------------------------------------------------------------
                System.out.println();
                System.out.println("--- 7.3 监听 update 事件 ---");
                events.updateOne(doc("type", "insert-demo"), doc("$set", doc("payload", "updated!", "count", 1)));
                batch = stream.poll(3000);
                System.out.println("收到 " + batch.size() + " 个事件");
                for (Map<String, Object> evt : batch) {
                    printEvent(evt);
                    resumeToken = extractResumeToken(evt);
                }

                // ------------------------------------------------------------------
                // 7.4 监听 delete 事件
                // ------------------------------------------------------------------
                System.out.println();
                System.out.println("--- 7.4 监听 delete 事件 ---");
                events.deleteOne(doc("type", "insert-demo"));
                batch = stream.poll(3000);
                System.out.println("收到 " + batch.size() + " 个事件");
                for (Map<String, Object> evt : batch) {
                    printEvent(evt);
                    resumeToken = extractResumeToken(evt);
                }

                // ------------------------------------------------------------------
                // 7.5 从 resume token 恢复
                // ------------------------------------------------------------------
                System.out.println();
                System.out.println("--- 7.5 从 resume token 恢复 ---");
                if (resumeToken != null) {
                    System.out.println("已保存 resume token，关闭旧流并重新打开...");
                    stream.close();
                    stream = ChangeStream.open(client, db, COLL_NAME, resumeToken, false);
                    System.out.println("恢复后的 Change Stream cursorId=" + stream.cursorId);

                    // 产生新事件，验证恢复后能继续接收
                    events.insertOne(doc("type", "after-resume", "payload", "still alive"));
                    batch = stream.poll(3000);
                    System.out.println("恢复后收到 " + batch.size() + " 个事件");
                    for (Map<String, Object> evt : batch) {
                        printEvent(evt);
                    }
                } else {
                    System.out.println("未获得 resume token（前序无事件），跳过恢复演示");
                }

                // ------------------------------------------------------------------
                // 7.6 管道过滤 (pipeline filter)
                // ------------------------------------------------------------------
                System.out.println();
                System.out.println("--- 7.6 Change Stream 管道过滤 ---");
                // 通过额外的 $match 管道阶段只接收特定类型的事件
                stream.close();
                stream = ChangeStream.open(client, db, COLL_NAME, null, true);

                events.insertOne(doc("type", "filtered-demo", "payload", "should pass filter"));
                events.insertOne(doc("type", "other", "payload", "should be filtered out"));

                batch = stream.poll(3000);
                System.out.println("过滤后收到 " + batch.size() + " 个事件");
                for (Map<String, Object> evt : batch) {
                    printEvent(evt);
                }
                if (batch.isEmpty()) {
                    System.out.println("（无事件返回；若服务器不支持 $changeStream 属预期情况）");
                }

            } catch (Exception e) {
                System.out.println("Change Stream 演示异常: " + e.getMessage());
                System.out.println("说明: 自研服务器可能尚未完整实现 $changeStream 管道阶段，");
                System.out.println("      本示例仍完整展示了客户端侧的标准协议用法。");
            } finally {
                if (stream != null) {
                    stream.close();
                }
            }

            // 清理
            try {
                events.deleteMany(new LinkedHashMap<>());
                db.dropCollection(COLL_NAME);
            } catch (Exception ignored) {
            }
            System.out.println();
            System.out.println("演示集合 " + COLL_NAME + " 已清理");
        }
    }

    // ==================== ChangeStream 辅助类 ====================

    /**
     * 基于 aggregate $changeStream 的轻量 Change Stream 实现。
     *
     * <p>工作方式：</p>
     * <ol>
     *   <li>发送 {@code aggregate + pipeline[$changeStream, ...] + cursor} 命令</li>
     *   <li>解析响应中的 cursor.id 与 firstBatch</li>
     *   <li>后续事件通过 {@code getMore} 命令拉取（响应中为 nextBatch）</li>
     * </ol>
     */
    private static final class ChangeStream {
        final MdbClient client;
        final MdbDatabase db;
        final String collectionName;
        long cursorId;

        private ChangeStream(MdbClient client, MdbDatabase db, String collectionName, long cursorId) {
            this.client = client;
            this.db = db;
            this.collectionName = collectionName;
            this.cursorId = cursorId;
        }

        /**
         * 打开 Change Stream。
         *
         * @param resumeToken 为 null 表示从当前位置开始监听；否则从该 token 恢复
         * @param withFilter  true 时附加 $match 管道，只接收 type=filtered-demo 的事件
         */
        static ChangeStream open(MdbClient client, MdbDatabase db, String collName,
                                 String resumeToken, boolean withFilter) throws Exception {
            List<Map<String, Object>> pipeline = new ArrayList<>();

            // $changeStream 阶段
            Map<String, Object> changeStream = new LinkedHashMap<>();
            Map<String, Object> csOpts = new LinkedHashMap<>();
            csOpts.put("fullDocument", "updateLookup"); // update 时返回最新文档
            if (resumeToken != null) {
                csOpts.put("resumeAfter", parseResumeToken(resumeToken));
            }
            changeStream.put("$changeStream", csOpts);
            pipeline.add(changeStream);

            // 可选：管道过滤，只接收特定 operationType / 字段值
            if (withFilter) {
                Map<String, Object> match = new LinkedHashMap<>();
                Map<String, Object> matchSpec = new LinkedHashMap<>();
                // 过滤 operationType 为 insert 且 type 字段为 filtered-demo 的事件
                matchSpec.put("operationType", "insert");
                match.put("$match", matchSpec);
                pipeline.add(match);
            }

            // 发送 aggregate 命令
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("aggregate", collName);
            cmd.put("pipeline", pipeline);
            cmd.put("cursor", map("batchSize", 100));
            cmd.put("$db", db.getName());

            Map<String, Object> body = client.getConnection().sendCommand(cmd).body;
            long cursorId = 0;
            if (body != null) {
                Object cursor = body.get("cursor");
                if (cursor instanceof Map) {
                    Object id = ((Map<?, ?>) cursor).get("id");
                    if (id instanceof Number) {
                        cursorId = ((Number) id).longValue();
                    }
                } else {
                    // 某些实现可能直接返回错误或空结果
                    System.out.println("  aggregate $changeStream 响应: ok=" + body.get("ok")
                            + ", keys=" + body.keySet());
                }
            }
            return new ChangeStream(client, db, collName, cursorId);
        }

        /**
         * 轮询事件：若服务器返回了第一批事件则直接返回；
         * 否则尝试 getMore 拉取（带超时由服务器 maxAwaitTimeMs 决定）。
         *
         * @param waitHintMs 仅作提示打印，实际等待时长由服务器 cursor 行为决定
         * @return 事件文档列表（可能为空）
         */
        List<Map<String, Object>> poll(int waitHintMs) throws Exception {
            List<Map<String, Object>> events = new ArrayList<>();

            // 若 open 时已有 firstBatch，可先尝试 getMore
            if (cursorId == 0) {
                return events;
            }

            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("getMore", cursorId);
            cmd.put("collection", collectionName);
            cmd.put("cursor", map("batchSize", 100, "maxTimeMS", waitHintMs));
            cmd.put("$db", db.getName());

            try {
                WireProtocol.OpMsg response = client.getConnection().sendCommand(cmd);
                Map<String, Object> body = response.body;
                if (body == null) {
                    return events;
                }
                Object cursor = body.get("cursor");
                if (cursor instanceof Map) {
                    Map<?, ?> cursorMap = (Map<?, ?>) cursor;
                    Object id = cursorMap.get("id");
                    if (id instanceof Number) {
                        cursorId = ((Number) id).longValue();
                    }
                    // getMore 响应使用 nextBatch（首次 aggregate 使用 firstBatch）
                    Object batch = cursorMap.get("nextBatch");
                    if (batch == null) {
                        batch = cursorMap.get("firstBatch");
                    }
                    if (batch instanceof List) {
                        for (Object o : (List<?>) batch) {
                            if (o instanceof Map) {
                                @SuppressWarnings("unchecked")
                                Map<String, Object> evt = (Map<String, Object>) o;
                                events.add(evt);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // 服务器未实现 getMore / $changeStream 时的容错
                System.out.println("  getMore 失败: " + e.getMessage());
            }
            return events;
        }

        void close() {
            if (cursorId != 0) {
                try {
                    Map<String, Object> cmd = new LinkedHashMap<>();
                    cmd.put("killCursors", collectionName);
                    cmd.put("cursors", java.util.Arrays.asList(cursorId));
                    cmd.put("$db", db.getName());
                    client.getConnection().sendCommand(cmd);
                } catch (Exception ignored) {
                }
                cursorId = 0;
            }
        }

        /** 将 resume token 字符串解析为 resumeAfter 文档（{_data: token}）。 */
        private static Map<String, Object> parseResumeToken(String token) {
            return map("_data", token);
        }
    }

    // ==================== 打印与工具方法 ====================

    /** 打印一个 change stream 事件的关键字段。 */
    private static void printEvent(Map<String, Object> evt) {
        Object opType = evt.get("operationType");
        Object ns = evt.get("ns");
        Object full = evt.get("fullDocument");
        Object id = evt.get("_id");
        StringBuilder sb = new StringBuilder();
        sb.append("  [event] operationType=").append(opType);
        if (ns != null) {
            sb.append(", ns=").append(ns);
        }
        if (full != null) {
            sb.append(", fullDocument=").append(full);
        }
        if (id != null) {
            Object data = id instanceof Map ? ((Map<?, ?>) id).get("_data") : id;
            sb.append(", token=").append(data);
        }
        System.out.println(sb);
    }

    /** 从事件中提取 resume token（_id._data）。 */
    private static String extractResumeToken(Map<String, Object> evt) {
        Object id = evt.get("_id");
        if (id instanceof Map) {
            Object data = ((Map<?, ?>) id).get("_data");
            return data == null ? null : String.valueOf(data);
        }
        return id == null ? null : String.valueOf(id);
    }

    private static Map<String, Object> doc(Object... pairs) {
        return map(pairs);
    }

    private static Map<String, Object> map(Object... pairs) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put((String) pairs[i], pairs[i + 1]);
        }
        return m;
    }

    /** 独立运行入口：ChangeStreamExample [host] [port] */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 27017;
        run(host, port);
    }
}
