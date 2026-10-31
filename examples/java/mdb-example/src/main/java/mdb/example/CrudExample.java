package mdb.example;

import mdb.driver.FindCursor;
import mdb.driver.MdbClient;
import mdb.driver.MdbCollection;
import mdb.driver.MdbDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * CRUD 示例：增删改查、过滤操作符、更新操作符、游标与聚合辅助能力。
 *
 * <p>自研驱动 API：</p>
 * <ul>
 *   <li>写入：insertOne / insertMany</li>
 *   <li>查询：find / findOne / distinct / countDocuments</li>
 *   <li>更新：updateOne / updateMany / findAndModify（runCommand）</li>
 *   <li>删除：deleteOne / deleteMany</li>
 * </ul>
 */
public final class CrudExample {

    private static final String DB_NAME = "mdb_example_db";
    private static final String COLL_NAME = "users";

    private CrudExample() {
    }

    public static void run(String host, int port) throws Exception {
        try (MdbClient client = new MdbClient(host, port)) {
            client.connect();
            MdbDatabase db = client.getDatabase(DB_NAME);
            MdbCollection users = db.getCollection(COLL_NAME);

            // 每次运行前清空集合，保证示例可重复执行
            try {
                db.dropCollection(COLL_NAME);
            } catch (Exception ignored) {
                // 集合可能尚不存在
            }

            // ------------------------------------------------------------------
            // 2.1 insertOne / insertMany
            // ------------------------------------------------------------------
            System.out.println("--- 2.1 插入文档 ---");
            // 单条插入：驱动若未提供 _id 会自动生成 ObjectId
            Map<String, Object> alice = doc(
                    "name", "Alice",
                    "age", 30,
                    "city", "Beijing",
                    "tags", Arrays.asList("java", "mdb"),
                    "active", true,
                    "score", 95.5
            );
            MdbCollection.InsertOneResult oneResult = users.insertOne(alice);
            System.out.println("insertOne: " + oneResult);

            // 批量插入
            List<Map<String, Object>> batch = new ArrayList<>();
            batch.add(doc("name", "Bob", "age", 25, "city", "Shanghai", "tags", Arrays.asList("go", "mdb"), "active", true, "score", 88.0));
            batch.add(doc("name", "Carol", "age", 28, "city", "Beijing", "tags", Arrays.asList("java"), "active", true, "score", 92.0));
            batch.add(doc("name", "Dave", "age", 35, "city", "Shenzhen", "tags", Arrays.asList("python"), "active", false, "score", 70.0));
            batch.add(doc("name", "Eve", "age", 22, "city", "Beijing", "tags", Arrays.asList("java", "go", "rust"), "active", true, "score", 99.0));
            batch.add(doc("name", "Frank", "age", 40, "city", "Shanghai", "tags", Arrays.asList("java"), "active", true, "score", 85.0));
            MdbCollection.InsertManyResult manyResult = users.insertMany(batch);
            System.out.println("insertMany: 插入 " + manyResult.insertedCount + " 条");

            // ------------------------------------------------------------------
            // 2.2 find / findOne
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 2.2 查询文档 ---");
            // findOne 返回第一条匹配文档，无匹配时返回 null
            Map<String, Object> found = users.findOne(map("name", "Alice"));
            System.out.println("findOne(name=Alice): " + found);

            // find 返回游标，可链式调用 sort/limit/skip/projection
            System.out.println("find() 全部文档:");
            FindCursor all = users.find(new LinkedHashMap<>());
            try {
                int count = 0;
                while (all.hasNext()) {
                    Map<String, Object> doc = all.next();
                    System.out.println("  " + doc.get("name") + " age=" + doc.get("age") + " city=" + doc.get("city"));
                    count++;
                }
                System.out.println("  共 " + count + " 条");
            } finally {
                all.close();
            }

            // ------------------------------------------------------------------
            // 2.3 过滤操作符：$eq $gt $gte $lt $lte $in $ne $and $or
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 2.3 过滤操作符 ---");

            // $eq：等于（与直接相等写法等价）
            printNames("age $eq 30", users.find(map("age", map("$eq", 30))));
            // $gt / $gte / $lt / $lte：数值范围
            printNames("age $gt 28", users.find(map("age", map("$gt", 28))));
            printNames("age $gte 30", users.find(map("age", map("$gte", 30))));
            printNames("age $lt 28", users.find(map("age", map("$lt", 28))));
            printNames("age $lte 25", users.find(map("age", map("$lte", 25))));
            // $in：字段值在给定列表中
            printNames("city $in [Beijing, Shenzhen]", users.find(map("city", map("$in", Arrays.asList("Beijing", "Shenzhen")))));
            // $ne：不等于
            printNames("active $ne true", users.find(map("active", map("$ne", true))));
            // $and：多个条件同时满足
            printNames("age $gt 25 AND city=Beijing",
                    users.find(map("$and", Arrays.asList(
                            map("age", map("$gt", 25)),
                            map("city", "Beijing")
                    ))));
            // $or：任一条件满足即可
            printNames("city=Beijing OR city=Shenzhen",
                    users.find(map("$or", Arrays.asList(
                            map("city", "Beijing"),
                            map("city", "Shenzhen")
                    ))));

            // ------------------------------------------------------------------
            // 2.4 sort / limit / skip / projection
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 2.4 sort / limit / skip / projection ---");

            // sort: 1 升序, -1 降序
            FindCursor sorted = users.find(new LinkedHashMap<>()).sort(map("age", 1)).limit(3);
            try {
                System.out.println("按 age 升序 top3:");
                while (sorted.hasNext()) {
                    Map<String, Object> d = sorted.next();
                    System.out.println("  " + d.get("name") + " -> " + d.get("age"));
                }
            } finally {
                sorted.close();
            }

            // skip: 跳过前 N 条（常与 sort 搭配做分页）
            FindCursor paged = users.find(new LinkedHashMap<>()).sort(map("age", 1)).skip(2).limit(2);
            try {
                System.out.println("分页: skip=2 limit=2:");
                while (paged.hasNext()) {
                    Map<String, Object> d = paged.next();
                    System.out.println("  " + d.get("name") + " age=" + d.get("age"));
                }
            } finally {
                paged.close();
            }

            // projection: 1 表示返回该字段, 0 表示排除该字段
            FindCursor projected = users.find(map("active", true))
                    .projection(map("name", 1, "city", 1))
                    .limit(3);
            try {
                System.out.println("投影 name+city:");
                while (projected.hasNext()) {
                    System.out.println("  " + projected.next());
                }
            } finally {
                projected.close();
            }

            // ------------------------------------------------------------------
            // 2.5 更新操作符：$set $unset $inc $push $pull $rename $min $max
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 2.5 更新操作符 ---");

            // $set：设置字段值
            MdbCollection.UpdateResult r1 = users.updateOne(
                    map("name", "Alice"),
                    map("$set", map("age", 31, "city", "Hangzhou"))
            );
            System.out.println("$set age/city: " + r1);

            // $unset：删除字段
            MdbCollection.UpdateResult r2 = users.updateOne(
                    map("name", "Bob"),
                    map("$unset", map("score", ""))
            );
            System.out.println("$unset score: " + r2);

            // $inc：数值自增（字段不存在时会创建并设为增量值）
            MdbCollection.UpdateResult r3 = users.updateOne(
                    map("name", "Bob"),
                    map("$inc", map("age", 1))
            );
            System.out.println("$inc age +1: " + r3);

            // $push：向数组字段追加元素
            MdbCollection.UpdateResult r4 = users.updateOne(
                    map("name", "Carol"),
                    map("$push", map("tags", "spring"))
            );
            System.out.println("$push tags: " + r4);

            // $pull：从数组字段移除指定元素
            MdbCollection.UpdateResult r5 = users.updateOne(
                    map("name", "Carol"),
                    map("$pull", map("tags", "java"))
            );
            System.out.println("$pull tags: " + r5);

            // $rename：重命名字段
            MdbCollection.UpdateResult r6 = users.updateOne(
                    map("name", "Dave"),
                    map("$rename", map("score", "grade"))
            );
            System.out.println("$rename score->grade: " + r6);

            // $min：仅当新值更小时更新
            users.insertOne(doc("name", "MinTest", "age", 50));
            MdbCollection.UpdateResult r7 = users.updateOne(
                    map("name", "MinTest"),
                    map("$min", map("age", 40))
            );
            System.out.println("$min age 40 (原50): " + r7);

            // $max：仅当新值更大时更新
            MdbCollection.UpdateResult r8 = users.updateOne(
                    map("name", "MinTest"),
                    map("$max", map("age", 60))
            );
            System.out.println("$max age 60 (原40): " + r8);

            // updateMany：批量更新
            MdbCollection.UpdateResult r9 = users.updateMany(
                    map("city", "Beijing"),
                    map("$set", map("region", "north"))
            );
            System.out.println("updateMany city=Beijing $set region: " + r9);

            // ------------------------------------------------------------------
            // 2.6 findAndModify（原子查找并修改，通过 runCommand）
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 2.6 findAndModify ---");
            // findAndModify: 匹配 query，执行 update，并返回修改前后的文档
            // new=true 返回更新后的文档，new=false 返回更新前的文档
            Map<String, Object> famCmd = new LinkedHashMap<>();
            famCmd.put("findAndModify", COLL_NAME);
            famCmd.put("query", map("name", "Eve"));
            famCmd.put("update", map("$inc", map("score", 1)));
            famCmd.put("new", true);
            Map<String, Object> famResult = db.runCommand(famCmd);
            Object value = famResult.get("value");
            System.out.println("findAndModify(new=true) score+1: value=" + value);

            // findAndModify + remove=true：查找并删除
            Map<String, Object> famRemove = new LinkedHashMap<>();
            famRemove.put("findAndModify", COLL_NAME);
            famRemove.put("query", map("name", "MinTest"));
            famRemove.put("remove", true);
            Map<String, Object> famRemoved = db.runCommand(famRemove);
            System.out.println("findAndModify(remove=true) MinTest: value=" + famRemoved.get("value"));

            // ------------------------------------------------------------------
            // 2.7 replaceOne —— 整文档替换（保留 _id）
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 2.7 replaceOne ---");
            // 思路：查出原 _id，删除旧行，再用相同 _id 插入新文档
            Map<String, Object> target = users.findOne(map("name", "Frank"));
            if (target != null) {
                Object id = target.get("_id");
                users.deleteOne(map("_id", id));
                Map<String, Object> replacement = new LinkedHashMap<>();
                replacement.put("_id", id); // 保留原 _id，实现“替换”语义
                replacement.put("name", "Frank");
                replacement.put("age", 41);
                replacement.put("city", "Chengdu");
                replacement.put("tags", Arrays.asList("java", "kotlin"));
                replacement.put("active", true);
                users.insertOne(replacement);
                System.out.println("已替换 Frank: " + users.findOne(map("name", "Frank")));
            } else {
                System.out.println("未找到 Frank，跳过 replace 示例");
            }

            // ------------------------------------------------------------------
            // 2.8 distinct / countDocuments
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 2.8 distinct / countDocuments ---");
            // distinct：返回字段的去重值列表
            Object cities = users.distinct("city", new LinkedHashMap<>());
            System.out.println("distinct(city) = " + cities);

            Object activeCities = users.distinct("city", map("active", true));
            System.out.println("distinct(city, active=true) = " + activeCities);

            // countDocuments：统计匹配数量
            long total = users.countDocuments(new LinkedHashMap<>());
            long beijing = users.countDocuments(map("city", "Beijing"));
            long young = users.countDocuments(map("age", map("$lt", 30)));
            System.out.println("countDocuments 全部 = " + total);
            System.out.println("countDocuments city=Beijing = " + beijing);
            System.out.println("countDocuments age<30 = " + young);

            // ------------------------------------------------------------------
            // 2.9 游标深度遍历（大结果集分批获取）
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 2.9 游标遍历 ---");
            // FindCursor 内部按 batch（默认约 101 条）通过 getMore 拉取数据
            for (int i = 0; i < 150; i++) {
                users.insertOne(doc("name", "Bulk_" + i, "age", 20 + (i % 30), "city", "BulkCity"));
            }
            long bulkCount = users.countDocuments(new LinkedHashMap<>());
            System.out.println("批量写入后总数: " + bulkCount);

            FindCursor cursor = users.find(map("city", "BulkCity")).sort(map("age", 1));
            int iterated = 0;
            try {
                while (cursor.hasNext()) {
                    Map<String, Object> d = cursor.next();
                    iterated++;
                    if (iterated <= 3 || iterated % 50 == 0) {
                        System.out.println("  第 " + iterated + " 条: " + d.get("name") + " age=" + d.get("age"));
                    }
                }
            } finally {
                cursor.close(); // 使用完毕关闭游标，释放服务器端 cursor
            }
            System.out.println("游标共迭代 " + iterated + " 条");

            // ------------------------------------------------------------------
            // 2.10 清理演示数据
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 2.10 清理 ---");
            MdbCollection.DeleteResult del = users.deleteMany(map("city", "BulkCity"));
            System.out.println("deleteMany city=BulkCity: " + del);
            users.deleteMany(new LinkedHashMap<>());
            System.out.println("集合已清空, count=" + users.countDocuments(new LinkedHashMap<>()));
        }
    }

    // ==================== 辅助方法 ====================

    /** 构造 LinkedHashMap 文档，参数为 key, value, key, value... */
    private static Map<String, Object> doc(Object... pairs) {
        return map(pairs);
    }

    /** 构造过滤/更新条件 Map。 */
    private static Map<String, Object> map(Object... pairs) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            m.put((String) pairs[i], pairs[i + 1]);
        }
        return m;
    }

    /** 打印查询结果中的 name 字段，便于观察过滤效果。 */
    private static void printNames(String label, FindCursor cursor) {
        System.out.print(label + " -> ");
        List<String> names = new ArrayList<>();
        try {
            while (cursor.hasNext()) {
                Object name = cursor.next().get("name");
                names.add(name == null ? "?" : String.valueOf(name));
            }
        } finally {
            cursor.close();
        }
        System.out.println(names);
    }

    /** 独立运行入口：CrudExample [host] [port] */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 27017;
        run(host, port);
    }
}
