package mdb.example;

import mdb.driver.MdbClient;
import mdb.driver.MdbCollection;
import mdb.driver.MdbDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 聚合管道示例：$match $group $sort $limit $skip $project $unwind $count 及管道串联。
 *
 * <p>聚合命令返回 cursor 文档，本示例从 cursor.firstBatch 中提取结果集。</p>
 */
public final class AggregationExample {

    private static final String DB_NAME = "mdb_example_db";
    private static final String COLL_NAME = "orders";

    private AggregationExample() {
    }

    public static void run(String host, int port) throws Exception {
        try (MdbClient client = new MdbClient(host, port)) {
            client.connect();
            MdbDatabase db = client.getDatabase(DB_NAME);
            MdbCollection orders = db.getCollection(COLL_NAME);

            try {
                db.dropCollection(COLL_NAME);
            } catch (Exception ignored) {
            }

            // ------------------------------------------------------------------
            // 4.1 准备订单数据
            // ------------------------------------------------------------------
            System.out.println("--- 4.1 准备订单数据 ---");
            List<Map<String, Object>> data = new ArrayList<>();
            // 每笔订单：customer, product, quantity, price, status, items[]
            data.add(doc("customer", "Alice", "product", "Laptop", "quantity", 1, "price", 5999.0, "status", "paid",
                    "items", Arrays.asList(doc("sku", "A001", "qty", 1), doc("sku", "ACC1", "qty", 2))));
            data.add(doc("customer", "Alice", "product", "Phone", "quantity", 1, "price", 3999.0, "status", "paid",
                    "items", Arrays.asList(doc("sku", "A002", "qty", 1))));
            data.add(doc("customer", "Bob", "product", "Book", "quantity", 3, "price", 59.0, "status", "paid",
                    "items", Arrays.asList(doc("sku", "B001", "qty", 3))));
            data.add(doc("customer", "Bob", "product", "Pen", "quantity", 10, "price", 5.0, "status", "shipped",
                    "items", Arrays.asList(doc("sku", "B002", "qty", 10))));
            data.add(doc("customer", "Carol", "product", "Monitor", "quantity", 2, "price", 1299.0, "status", "paid",
                    "items", Arrays.asList(doc("sku", "C001", "qty", 2), doc("sku", "ACC2", "qty", 1))));
            data.add(doc("customer", "Dave", "product", "Laptop", "quantity", 1, "price", 5999.0, "status", "cancelled",
                    "items", Arrays.asList(doc("sku", "A001", "qty", 1))));
            data.add(doc("customer", "Eve", "product", "Phone", "quantity", 2, "price", 3999.0, "status", "paid",
                    "items", Arrays.asList(doc("sku", "A002", "qty", 2))));
            MdbCollection.InsertManyResult inserted = orders.insertMany(data);
            System.out.println("插入 " + inserted.insertedCount + " 条订单");

            // ------------------------------------------------------------------
            // 4.2 $match —— 过滤
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 4.2 $match ---");
            // $match 相当于 find 的 filter，应尽量放在管道最前以利用索引
            List<Map<String, Object>> pipeline = new ArrayList<>();
            pipeline.add(stage("$match", map("status", "paid")));
            printDocs("$match status=paid", orders.aggregate(pipeline));

            // ------------------------------------------------------------------
            // 4.3 $group —— $sum $avg $min $max $push
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 4.3 $group 聚合 ---");

            // 按 customer 分组，统计订单数、总金额、平均金额
            pipeline = new ArrayList<>();
            Map<String, Object> groupStage = new LinkedHashMap<>();
            Map<String, Object> groupSpec = new LinkedHashMap<>();
            groupSpec.put("_id", "$customer");            // 分组键：customer 字段值
            groupSpec.put("orderCount", map("$sum", 1));  // 计数
            groupSpec.put("totalAmount", map("$sum", map("$multiply", Arrays.asList("$quantity", "$price"))));
            // 注意：部分服务器版本对 $multiply 支持不同，下面改用 $sum quantity 与 total 的通用写法
            groupStage.put("$group", groupSpec);
            pipeline.add(groupStage);
            try {
                printDocs("$group by customer (sum)", orders.aggregate(pipeline));
            } catch (Exception e) {
                System.out.println("$group + $multiply 失败: " + e.getMessage() + "，改用基础聚合器");
            }

            // 基础聚合器演示（与服务器实现对齐）
            pipeline = new ArrayList<>();
            Map<String, Object> g = new LinkedHashMap<>();
            Map<String, Object> gspec = new LinkedHashMap<>();
            gspec.put("_id", "$customer");
            gspec.put("orderCount", map("$sum", 1));
            gspec.put("totalQuantity", map("$sum", "$quantity"));
            gspec.put("avgPrice", map("$avg", "$price"));
            gspec.put("minPrice", map("$min", "$price"));
            gspec.put("maxPrice", map("$max", "$price"));
            gspec.put("products", map("$push", "$product"));
            g.put("$group", gspec);
            pipeline.add(g);
            printDocs("$group with $sum/$avg/$min/$max/$push", orders.aggregate(pipeline));

            // ------------------------------------------------------------------
            // 4.4 $sort / $limit / $skip
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 4.4 $sort / $limit / $skip ---");

            // $sort：按 price 降序
            pipeline = new ArrayList<>();
            pipeline.add(stage("$match", map("status", "paid")));
            pipeline.add(stage("$sort", map("price", -1)));
            printDocs("$sort price desc", orders.aggregate(pipeline));

            // $sort + $limit：取最贵的 3 笔
            pipeline = new ArrayList<>();
            pipeline.add(stage("$match", map("status", "paid")));
            pipeline.add(stage("$sort", map("price", -1)));
            pipeline.add(stage("$limit", 3));
            printDocs("$sort price desc + $limit 3", orders.aggregate(pipeline));

            // $skip + $limit：分页（跳过前 2 条，取 3 条）
            pipeline = new ArrayList<>();
            pipeline.add(stage("$sort", map("price", 1)));
            pipeline.add(stage("$skip", 2));
            pipeline.add(stage("$limit", 3));
            printDocs("$sort price asc + $skip 2 + $limit 3", orders.aggregate(pipeline));

            // ------------------------------------------------------------------
            // 4.5 $project —— 字段投影
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 4.5 $project ---");
            pipeline = new ArrayList<>();
            pipeline.add(stage("$match", map("status", "paid")));
            // 1 包含字段, 0 排除字段; "$field" 表达式可重命名或计算
            Map<String, Object> project = new LinkedHashMap<>();
            project.put("customer", 1);
            project.put("product", 1);
            project.put("amount", map("$multiply", Arrays.asList("$quantity", "$price")));
            project.put("_id", 0); // 不返回 _id
            pipeline.add(stage("$project", project));
            try {
                printDocs("$project with $multiply amount", orders.aggregate(pipeline));
            } catch (Exception e) {
                System.out.println("$project + $multiply 失败: " + e.getMessage());
                // 退化演示：仅投影字段
                pipeline = new ArrayList<>();
                pipeline.add(stage("$match", map("status", "paid")));
                pipeline.add(stage("$project", map("customer", 1, "product", 1, "quantity", 1, "price", 1, "_id", 0)));
                printDocs("$project basic fields", orders.aggregate(pipeline));
            }

            // ------------------------------------------------------------------
            // 4.6 $unwind —— 展开数组字段
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 4.6 $unwind ---");
            // 将 items 数组拆分为多条文档，每条文档对应数组中的一个元素
            pipeline = new ArrayList<>();
            pipeline.add(stage("$unwind", "$items"));
            pipeline.add(stage("$project", map(
                    "customer", 1,
                    "product", 1,
                    "sku", "$items.sku",
                    "qty", "$items.qty",
                    "_id", 0
            )));
            printDocs("$unwind items + $project sku/qty", orders.aggregate(pipeline));

            // ------------------------------------------------------------------
            // 4.7 $count —— 计数
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 4.7 $count ---");
            pipeline = new ArrayList<>();
            pipeline.add(stage("$match", map("status", "paid")));
            pipeline.add(stage("$count", "paidOrders"));
            printDocs("$count paidOrders", orders.aggregate(pipeline));

            // ------------------------------------------------------------------
            // 4.8 管道串联 —— 多阶段组合
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 4.8 管道串联 ---");
            // 完整业务分析：已支付订单 -> 展开 items -> 按 sku 分组 -> 统计销量 -> 按销量排序
            pipeline = new ArrayList<>();
            pipeline.add(stage("$match", map("status", "paid")));           // 1. 过滤已支付
            pipeline.add(stage("$unwind", "$items"));                      // 2. 展开明细
            pipeline.add(stage("$group", map(
                    "_id", "$items.sku",
                    "totalQty", map("$sum", "$items.qty"),
                    "customers", map("$push", "$customer")
            )));                                                            // 3. 按 sku 汇总
            pipeline.add(stage("$sort", map("totalQty", -1)));             // 4. 按销量降序
            pipeline.add(stage("$limit", 5));                              // 5. 取 Top5
            pipeline.add(stage("$project", map(
                    "sku", "$_id",
                    "totalQty", 1,
                    "customerCount", map("$size", "$customers"),
                    "_id", 0
            )));                                                            // 6. 整理输出
            try {
                printDocs("复杂分析: sku 销量 Top5", orders.aggregate(pipeline));
            } catch (Exception e) {
                System.out.println("复杂管道失败: " + e.getMessage());
                // 服务器可能不支持 $size 等表达式，用基础版管道
                pipeline = new ArrayList<>();
                pipeline.add(stage("$match", map("status", "paid")));
                pipeline.add(stage("$unwind", "$items"));
                pipeline.add(stage("$group", map(
                        "_id", "$items.sku",
                        "totalQty", map("$sum", "$items.qty")
                )));
                pipeline.add(stage("$sort", map("totalQty", -1)));
                pipeline.add(stage("$limit", 5));
                printDocs("复杂分析(基础版): sku 销量 Top5", orders.aggregate(pipeline));
            }

            // ------------------------------------------------------------------
            // 4.9 $match + $group 组合统计
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 4.9 $match + $group 组合 ---");
            pipeline = new ArrayList<>();
            pipeline.add(stage("$match", map("quantity", map("$gte", 2))));
            pipeline.add(stage("$group", map(
                    "_id", "$status",
                    "count", map("$sum", 1),
                    "avgQty", map("$avg", "$quantity")
            )));
            printDocs("$match qty>=2 + $group by status", orders.aggregate(pipeline));

            // 清理
            db.dropCollection(COLL_NAME);
            System.out.println();
            System.out.println("演示集合 " + COLL_NAME + " 已清理");
        }
    }

    // ==================== 辅助方法 ====================

    /** 构造聚合阶段：{stageName: spec} */
    private static Map<String, Object> stage(String name, Object spec) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put(name, spec);
        return m;
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

    /** 从 aggregate 响应中提取 cursor.firstBatch 并打印。 */
    @SuppressWarnings("unchecked")
    private static void printDocs(String label, Map<String, Object> body) {
        System.out.println(label + ":");
        if (body == null) {
            System.out.println("  (null response)");
            return;
        }
        Object cursor = body.get("cursor");
        List<Map<String, Object>> docs = null;
        if (cursor instanceof Map) {
            Object batch = ((Map<String, Object>) cursor).get("firstBatch");
            if (batch instanceof List) {
                docs = (List<Map<String, Object>>) batch;
            }
        }
        if (docs == null || docs.isEmpty()) {
            System.out.println("  (无结果) ok=" + body.get("ok"));
            return;
        }
        for (Map<String, Object> d : docs) {
            System.out.println("  " + d);
        }
        System.out.println("  共 " + docs.size() + " 条");
    }

    /** 独立运行入口：AggregationExample [host] [port] */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 27017;
        run(host, port);
    }
}
