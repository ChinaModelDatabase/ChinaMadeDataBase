package mdb.example;

import mdb.driver.MdbClient;
import mdb.driver.MdbCollection;
import mdb.driver.MdbDatabase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 管理操作示例：用户管理、集合/数据库统计、执行计划与数据校验。
 *
 * <p>管理命令通过 db.runCommand / client 直接发送：</p>
 * <ul>
 *   <li>用户：createUser / dropUser / grantRolesToUser / usersInfo</li>
 *   <li>统计：collStats / dbStats / serverStatus</li>
 *   <li>诊断：explain / validate</li>
 * </ul>
 *
 * <p>用户管理命令通常需要 userAdmin 或 root 角色。</p>
 */
public final class AdminExample {

    private static final String DB_NAME = "mdb_example_db";
    private static final String COLL_NAME = "admin_demo";
    private static final String USERNAME = "example_app_user";

    private AdminExample() {
    }

    public static void run(String host, int port) throws Exception {
        try (MdbClient client = new MdbClient(host, port)) {
            client.connect();
            MdbDatabase db = client.getDatabase(DB_NAME);
            MdbCollection coll = db.getCollection(COLL_NAME);

            try {
                db.dropCollection(COLL_NAME);
            } catch (Exception ignored) {
            }

            // ------------------------------------------------------------------
            // 8.1 准备演示集合
            // ------------------------------------------------------------------
            System.out.println("--- 8.1 准备演示集合 ---");
            List<Map<String, Object>> docs = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                docs.add(doc(
                        "name", "item_" + i,
                        "category", i % 2 == 0 ? "alpha" : "beta",
                        "value", i * 10
                ));
            }
            MdbCollection.InsertManyResult r = coll.insertMany(docs);
            System.out.println("插入 " + r.insertedCount + " 条演示数据");

            // ------------------------------------------------------------------
            // 8.2 createUser —— 创建用户
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 8.2 createUser ---");
            try {
                // MdbDatabase.createUser 内置 roles=readWrite
                db.createUser(USERNAME, "AppPassw0rd!");
                System.out.println("用户 " + USERNAME + " 创建成功 (roles=[readWrite])");
            } catch (Exception e) {
                System.out.println("createUser 失败（可能无权限或用户已存在）: " + e.getMessage());
            }

            // ------------------------------------------------------------------
            // 8.3 listUsers —— 查询用户信息 (usersInfo)
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 8.3 listUsers (usersInfo) ---");
            try {
                // usersInfo: 1 表示列出当前库所有用户
                Map<String, Object> allUsersCmd = new LinkedHashMap<>();
                allUsersCmd.put("usersInfo", 1);
                Map<String, Object> allUsers = db.runCommand(allUsersCmd);
                System.out.println("usersInfo(1): " + allUsers);

                // usersInfo: "username" 查询指定用户
                Map<String, Object> oneUserCmd = new LinkedHashMap<>();
                oneUserCmd.put("usersInfo", USERNAME);
                Map<String, Object> oneUser = db.runCommand(oneUserCmd);
                System.out.println("usersInfo(" + USERNAME + "): " + oneUser);
            } catch (Exception e) {
                System.out.println("usersInfo 失败: " + e.getMessage());
            }

            // ------------------------------------------------------------------
            // 8.4 grantRolesToUser —— 授予角色
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 8.4 grantRolesToUser ---");
            try {
                Map<String, Object> grantCmd = new LinkedHashMap<>();
                grantCmd.put("grantRolesToUser", USERNAME);
                // 角色格式: [{role: "角色名", db: "所在库"}]
                List<Map<String, Object>> roles = new ArrayList<>();
                roles.add(doc("role", "read", "db", DB_NAME));
                roles.add(doc("role", "dbAdmin", "db", DB_NAME));
                grantCmd.put("roles", roles);
                Map<String, Object> grantResult = db.runCommand(grantCmd);
                System.out.println("grantRolesToUser(read+dbAdmin): " + grantResult);

                // 查看授予后的用户信息
                Map<String, Object> infoCmd = new LinkedHashMap<>();
                infoCmd.put("usersInfo", USERNAME);
                Map<String, Object> info = db.runCommand(infoCmd);
                System.out.println("授予后 usersInfo: " + info);
            } catch (Exception e) {
                System.out.println("grantRolesToUser 失败: " + e.getMessage());
            }

            // ------------------------------------------------------------------
            // 8.5 collStats —— 集合统计
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 8.5 collStats ---");
            Map<String, Object> collStats = coll.getStats();
            System.out.println("collStats(" + COLL_NAME + "):");
            printSelected(collStats, "ns", "count", "size", "storageSize",
                    "totalIndexSize", "nindexes", "avgObjSize", "ok");

            // ------------------------------------------------------------------
            // 8.6 dbStats —— 数据库统计
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 8.6 dbStats ---");
            Map<String, Object> dbStats = db.getStats();
            System.out.println("dbStats(" + DB_NAME + "):");
            printSelected(dbStats, "db", "collections", "objects", "dataSize",
                    "storageSize", "indexes", "indexSize", "ok");

            // ------------------------------------------------------------------
            // 8.7 serverStatus —— 服务器状态
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 8.7 serverStatus ---");
            Map<String, Object> serverStatus = client.getServerStatus();
            System.out.println("serverStatus 关键字段:");
            printSelected(serverStatus, "ok", "host", "version", "process",
                    "uptime", "pid", "connections");

            // ------------------------------------------------------------------
            // 8.8 explain —— 查询执行计划
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 8.8 explain ---");
            // explain 返回查询计划、索引使用情况、执行阶段等信息
            Map<String, Object> explainResult = coll.explain(map("category", "alpha"));
            System.out.println("explain(category=alpha) 节选:");
            printBrief(explainResult, 8);

            // 无索引字段的 explain 对比
            Map<String, Object> explainNoIndex = coll.explain(map("value", map("$gt", 100)));
            System.out.println("explain(value>$gt 100) 节选:");
            printBrief(explainNoIndex, 6);

            // ------------------------------------------------------------------
            // 8.9 validate —— 数据校验
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 8.9 validate ---");
            try {
                Map<String, Object> validateCmd = new LinkedHashMap<>();
                validateCmd.put("validate", COLL_NAME);
                validateCmd.put("full", true); // 完整校验（检查结构一致性）
                Map<String, Object> validateResult = db.runCommand(validateCmd);
                System.out.println("validate(" + COLL_NAME + ", full=true):");
                printBrief(validateResult, 8);
            } catch (Exception e) {
                System.out.println("validate 失败: " + e.getMessage());
            }

            // ------------------------------------------------------------------
            // 8.10 listCollections / dropCollection —— 集合管理
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 8.10 集合管理 ---");
            List<String> collections = db.listCollections();
            System.out.println("当前库集合列表 (" + DB_NAME + "): " + collections);

            db.dropCollection(COLL_NAME);
            collections = db.listCollections();
            System.out.println("dropCollection 后: " + collections);

            // ------------------------------------------------------------------
            // 8.11 dropUser —— 删除用户（放在最后，避免影响前面的权限演示）
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 8.11 dropUser ---");
            try {
                Map<String, Object> dropUserCmd = new LinkedHashMap<>();
                dropUserCmd.put("dropUser", USERNAME);
                Map<String, Object> dropResult = db.runCommand(dropUserCmd);
                System.out.println("dropUser(" + USERNAME + "): " + dropResult);

                // 确认用户已删除
                Map<String, Object> confirmCmd = new LinkedHashMap<>();
                confirmCmd.put("usersInfo", USERNAME);
                Map<String, Object> confirm = db.runCommand(confirmCmd);
                System.out.println("删除后 usersInfo: " + confirm);
            } catch (Exception e) {
                System.out.println("dropUser 失败: " + e.getMessage());
            }

            System.out.println();
            System.out.println("管理操作示例完成");
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

    /** 打印 Map 中指定的字段。 */
    private static void printSelected(Map<String, Object> map, String... keys) {
        if (map == null) {
            System.out.println("  (null)");
            return;
        }
        for (String key : keys) {
            if (map.containsKey(key)) {
                Object value = map.get(key);
                // 长文档字段做截断，保持输出简洁
                String text = String.valueOf(value);
                if (text.length() > 120) {
                    text = text.substring(0, 120) + "...";
                }
                System.out.println("  " + key + " = " + text);
            }
        }
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
            String text = String.valueOf(e.getValue());
            if (text.length() > 120) {
                text = text.substring(0, 120) + "...";
            }
            System.out.println("  " + e.getKey() + " = " + text);
        }
    }

    /** 独立运行入口：AdminExample [host] [port] */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 27017;
        run(host, port);
    }
}
