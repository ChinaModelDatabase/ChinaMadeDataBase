package mdb.example;

import mdb.driver.MdbClient;
import mdb.driver.MdbCollection;
import mdb.driver.MdbDatabase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 索引管理示例：单字段、复合、唯一、TTL、文本索引，以及索引的查看与删除。
 *
 * <p>驱动提供 createIndex(keys) 便捷方法；带选项（unique / expireAfterSeconds / text）
 * 的索引通过 runCommand 发送完整 createIndexes 命令实现。</p>
 */
public final class IndexExample {

    private static final String DB_NAME = "mdb_example_db";
    private static final String COLL_NAME = "products";

    private IndexExample() {
    }

    public static void run(String host, int port) throws Exception {
        try (MdbClient client = new MdbClient(host, port)) {
            client.connect();
            MdbDatabase db = client.getDatabase(DB_NAME);
            MdbCollection products = db.getCollection(COLL_NAME);

            // 清空集合与旧索引，便于重复运行
            try {
                db.dropCollection(COLL_NAME);
            } catch (Exception ignored) {
            }

            // ------------------------------------------------------------------
            // 3.1 准备示例数据
            // ------------------------------------------------------------------
            System.out.println("--- 3.1 准备示例数据 ---");
            List<Map<String, Object>> docs = new ArrayList<>();
            docs.add(doc("sku", "A001", "name", "Laptop", "category", "electronics", "price", 5999, "stock", 10, "description", "高性能笔记本电脑"));
            docs.add(doc("sku", "A002", "name", "Phone", "category", "electronics", "price", 3999, "stock", 25, "description", "智能手机旗舰机型"));
            docs.add(doc("sku", "B001", "name", "Book", "category", "books", "price", 59, "stock", 100, "description", "数据库系统概念"));
            docs.add(doc("sku", "B002", "name", "Pen", "category", "stationery", "price", 5, "stock", 500, "description", "中性笔书写工具"));
            docs.add(doc("sku", "C001", "name", "Monitor", "category", "electronics", "price", 1299, "stock", 15, "description", "27寸显示器"));
            MdbCollection.InsertManyResult insertResult = products.insertMany(docs);
            System.out.println("插入 " + insertResult.insertedCount + " 条商品数据");

            // ------------------------------------------------------------------
            // 3.2 createIndex —— 单字段索引
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 3.2 单字段索引 ---");
            // 驱动便捷方法：keys 为字段名 -> 排序方向 (1 升序 / -1 降序)
            Map<String, Object> skuKeys = new LinkedHashMap<>();
            skuKeys.put("sku", 1);
            products.createIndex(skuKeys);
            System.out.println("已创建 sku 升序索引");

            // ------------------------------------------------------------------
            // 3.3 createIndex —— 复合索引
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 3.3 复合索引 ---");
            // 多字段复合索引：category 升序 + price 降序
            // 复合索引字段顺序影响查询效率：等值条件字段在前，范围条件字段在后
            Map<String, Object> compoundKeys = new LinkedHashMap<>();
            compoundKeys.put("category", 1);
            compoundKeys.put("price", -1);
            products.createIndex(compoundKeys);
            System.out.println("已创建 category+price 复合索引");

            // ------------------------------------------------------------------
            // 3.4 createIndex —— 唯一索引 (unique)
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 3.4 唯一索引 ---");
            // 唯一索引要求字段值不可重复，插入重复值会报错
            // 通过 runCommand 发送带 unique 选项的 createIndexes 命令
            try {
                Map<String, Object> cmd = new LinkedHashMap<>();
                cmd.put("createIndexes", COLL_NAME);
                List<Map<String, Object>> indexes = new ArrayList<>();

                Map<String, Object> idx = new LinkedHashMap<>();
                idx.put("key", map("sku", 1));
                idx.put("name", "sku_1_unique");
                idx.put("unique", true);
                indexes.add(idx);

                cmd.put("indexes", indexes);
                Map<String, Object> result = db.runCommand(cmd);
                System.out.println("已创建唯一索引 sku_1_unique: " + result);

                // 验证唯一性：插入重复 sku 应失败
                try {
                    products.insertOne(doc("sku", "A001", "name", "Dup"));
                    System.out.println("警告: 重复 sku 插入未报错（服务器可能未启用唯一约束）");
                } catch (Exception e) {
                    System.out.println("重复 sku 插入被拒绝（符合唯一约束）: " + e.getMessage());
                }
            } catch (Exception e) {
                System.out.println("创建唯一索引失败: " + e.getMessage());
            }

            // ------------------------------------------------------------------
            // 3.5 createIndex —— TTL 索引（过期自动删除）
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 3.5 TTL 索引 ---");
            // TTL 索引建立在 Date 类型字段上，服务器会自动删除超过 expireAfterSeconds 的文档
            // 典型场景：会话 token、日志、临时缓存
            try {
                Map<String, Object> ttlCmd = new LinkedHashMap<>();
                ttlCmd.put("createIndexes", COLL_NAME);
                List<Map<String, Object>> ttlIndexes = new ArrayList<>();

                Map<String, Object> ttlIdx = new LinkedHashMap<>();
                ttlIdx.put("key", map("expireAt", 1));
                ttlIdx.put("name", "expireAt_ttl");
                ttlIdx.put("expireAfterSeconds", 3600); // 1 小时后过期
                ttlIndexes.add(ttlIdx);

                ttlCmd.put("indexes", ttlIndexes);
                Map<String, Object> ttlResult = db.runCommand(ttlCmd);
                System.out.println("已创建 TTL 索引 expireAt_ttl (1h): " + ttlResult);

                // 插入带过期时间的文档
                Map<String, Object> tmp = doc(
                        "sku", "TTL001",
                        "name", "Temporary",
                        "category", "session",
                        "expireAt", new java.util.Date(System.currentTimeMillis() + 3600_000L)
                );
                products.insertOne(tmp);
                System.out.println("已插入带 expireAt 的临时文档");
            } catch (Exception e) {
                System.out.println("创建 TTL 索引失败: " + e.getMessage());
            }

            // ------------------------------------------------------------------
            // 3.6 createIndex —— 文本索引 (text)
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 3.6 文本索引 ---");
            // 文本索引用于全文检索，配合 $text 操作符使用
            try {
                Map<String, Object> textCmd = new LinkedHashMap<>();
                textCmd.put("createIndexes", COLL_NAME);
                List<Map<String, Object>> textIndexes = new ArrayList<>();

                Map<String, Object> textIdx = new LinkedHashMap<>();
                textIdx.put("key", map("description", "text"));
                textIdx.put("name", "description_text");
                textIndexes.add(textIdx);

                textCmd.put("indexes", textIndexes);
                Map<String, Object> textResult = db.runCommand(textCmd);
                System.out.println("已创建文本索引 description_text: " + textResult);

                // 文本检索示例
                Map<String, Object> textQuery = new LinkedHashMap<>();
                textQuery.put("find", COLL_NAME);
                textQuery.put("filter", map("$text", map("$search", "笔记本")));
                textQuery.put("$db", DB_NAME);
                Map<String, Object> textSearch = client.getConnection().sendCommand(textQuery).body;
                System.out.println("全文检索 $text search=笔记本: " + summarize(textSearch));
            } catch (Exception e) {
                System.out.println("创建/使用文本索引失败: " + e.getMessage());
            }

            // ------------------------------------------------------------------
            // 3.7 listIndexes —— 查看集合上的全部索引
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 3.7 listIndexes ---");
            List<Map<String, Object>> indexes = products.listIndexes();
            System.out.println("当前索引数量: " + indexes.size());
            for (Map<String, Object> idx : indexes) {
                System.out.println("  - name=" + idx.get("name") + ", key=" + idx.get("key")
                        + (idx.containsKey("unique") ? ", unique=" + idx.get("unique") : "")
                        + (idx.containsKey("expireAfterSeconds") ? ", expireAfterSeconds=" + idx.get("expireAfterSeconds") : ""));
            }

            // ------------------------------------------------------------------
            // 3.8 dropIndex —— 删除索引
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 3.8 dropIndex ---");
            try {
                products.dropIndex("description_text");
                System.out.println("已删除文本索引 description_text");
            } catch (Exception e) {
                System.out.println("删除文本索引失败: " + e.getMessage());
            }

            List<Map<String, Object>> afterDrop = products.listIndexes();
            System.out.println("删除后索引数量: " + afterDrop.size());
            for (Map<String, Object> idx : afterDrop) {
                System.out.println("  - " + idx.get("name"));
            }

            // ------------------------------------------------------------------
            // 3.9 索引统计信息 (collStats)
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 3.9 索引统计 (collStats) ---");
            Map<String, Object> stats = products.getStats();
            System.out.println("collStats 关键字段:");
            for (String key : new String[]{"ns", "count", "size", "storageSize", "totalIndexSize", "nindexes", "ok"}) {
                if (stats.containsKey(key)) {
                    System.out.println("  " + key + " = " + stats.get(key));
                }
            }

            // ------------------------------------------------------------------
            // 3.10 索引使用效果对比（explain）
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 3.10 explain 执行计划 ---");
            Map<String, Object> explainResult = products.explain(map("category", "electronics"));
            System.out.println("explain(category=electronics) 节选:");
            printBrief(explainResult, 6);

            // 清理
            db.dropCollection(COLL_NAME);
            System.out.println();
            System.out.println("演示集合 " + COLL_NAME + " 已清理");
        }
    }

    // ==================== 辅助方法 ====================

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

    /** 简要打印命令返回中的 ok 与若干字段。 */
    private static String summarize(Map<String, Object> body) {
        if (body == null) {
            return "null";
        }
        Object cursor = body.get("cursor");
        if (cursor instanceof Map) {
            Object firstBatch = ((Map<?, ?>) cursor).get("firstBatch");
            return "ok=" + body.get("ok") + ", firstBatch=" + firstBatch;
        }
        return "ok=" + body.get("ok") + ", keys=" + body.keySet();
    }

    /** 打印 Map 的前 n 个字段。 */
    private static void printBrief(Map<String, Object> map, int n) {
        if (map == null) {
            System.out.println("  (null)");
            return;
        }
        int i = 0;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (i++ >= n) {
                System.out.println("  ...");
                break;
            }
            System.out.println("  " + e.getKey() + " = " + e.getValue());
        }
    }

    /** 独立运行入口：IndexExample [host] [port] */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 27017;
        run(host, port);
    }
}
