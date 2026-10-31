package mdb.example;

import mdb.driver.MdbClient;
import mdb.driver.MdbCollection;
import mdb.driver.MdbDatabase;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 事务示例：事务提交/回滚、WriteConcern、ReadConcern、可重试写入。
 *
 * <p>自研驱动通过 runCommand 发送标准事务命令：</p>
 * <ul>
 *   <li>startTransaction / commitTransaction / abortTransaction</li>
 *   <li>写入命令可携带 lsid + txnNumber 以支持可重试写入</li>
 *   <li>WriteConcern: {w: 1} 或 {w: "majority"}</li>
 *   <li>ReadConcern: {level: "local"} 或 {level: "majority"}</li>
 * </ul>
 *
 * <p>说明：事务命令在服务器侧需配套存储引擎支持；示例对每次操作都做容错处理，
 * 即使服务器仅返回 ok=1 的 stub 响应，也能完整演示协议用法。</p>
 */
public final class TransactionExample {

    private static final String DB_NAME = "mdb_example_db";
    private static final String ACCOUNTS = "accounts";

    private TransactionExample() {
    }

    public static void run(String host, int port) throws Exception {
        try (MdbClient client = new MdbClient(host, port)) {
            client.connect();
            MdbDatabase db = client.getDatabase(DB_NAME);
            MdbCollection accounts = db.getCollection(ACCOUNTS);

            try {
                db.dropCollection(ACCOUNTS);
            } catch (Exception ignored) {
            }

            // ------------------------------------------------------------------
            // 5.1 准备账户数据
            // ------------------------------------------------------------------
            System.out.println("--- 5.1 准备账户数据 ---");
            accounts.insertOne(doc("account", "alice", "balance", 1000.0));
            accounts.insertOne(doc("account", "bob", "balance", 500.0));
            System.out.println("alice balance=" + accounts.findOne(map("account", "alice")).get("balance"));
            System.out.println("bob   balance=" + accounts.findOne(map("account", "bob")).get("balance"));

            // ------------------------------------------------------------------
            // 5.2 基本事务（提交）—— 模拟转账
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 5.2 基本事务 (commit) ---");
            System.out.println("业务场景: alice 向 bob 转账 100，两笔扣款/入账必须同时成功或同时失败");
            try {
                // 1) 开启事务
                Map<String, Object> startTxn = new LinkedHashMap<>();
                startTxn.put("startTransaction", 1);
                Map<String, Object> startResult = db.runCommand(startTxn);
                System.out.println("startTransaction: ok=" + startResult.get("ok"));

                // 2) 事务内写入（alice 扣款）
                Map<String, Object> debit = doc("$inc", map("balance", -100.0));
                MdbCollection.UpdateResult debitResult = accounts.updateOne(map("account", "alice"), debit);
                System.out.println("事务内 alice $inc -100: " + debitResult);

                // 3) 事务内写入（bob 入账）
                MdbCollection.UpdateResult creditResult = accounts.updateOne(
                        map("account", "bob"),
                        map("$inc", map("balance", 100.0))
                );
                System.out.println("事务内 bob $inc +100: " + creditResult);

                // 4) 提交事务
                Map<String, Object> commitTxn = new LinkedHashMap<>();
                commitTxn.put("commitTransaction", 1);
                Map<String, Object> commitResult = db.runCommand(commitTxn);
                System.out.println("commitTransaction: ok=" + commitResult.get("ok"));
            } catch (Exception e) {
                System.out.println("事务提交示例失败: " + e.getMessage());
            }

            System.out.println("事务后 alice=" + accounts.findOne(map("account", "alice")).get("balance")
                    + ", bob=" + accounts.findOne(map("account", "bob")).get("balance"));

            // ------------------------------------------------------------------
            // 5.3 事务中止（abort）—— 失败时回滚全部写入
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 5.3 事务中止 (abort) ---");
            double aliceBefore = ((Number) accounts.findOne(map("account", "alice")).get("balance")).doubleValue();
            try {
                Map<String, Object> startTxn = new LinkedHashMap<>();
                startTxn.put("startTransaction", 1);
                db.runCommand(startTxn);
                System.out.println("startTransaction: ok");

                // 事务内执行扣款（后续将被回滚）
                accounts.updateOne(map("account", "alice"), map("$inc", map("balance", -999.0)));
                System.out.println("事务内 alice $inc -999（尚未提交）");

                // 模拟业务校验失败，主动中止事务
                Map<String, Object> abortTxn = new LinkedHashMap<>();
                abortTxn.put("abortTransaction", 1);
                Map<String, Object> abortResult = db.runCommand(abortTxn);
                System.out.println("abortTransaction: ok=" + abortResult.get("ok"));
            } catch (Exception e) {
                System.out.println("事务中止示例失败: " + e.getMessage());
            }

            double aliceAfter = ((Number) accounts.findOne(map("account", "alice")).get("balance")).doubleValue();
            System.out.println("中止前 alice=" + aliceBefore + " / 中止后 alice=" + aliceAfter);
            if (aliceAfter == aliceBefore) {
                System.out.println("回滚生效: 余额未变化");
            } else {
                System.out.println("注意: 余额发生变化（服务器可能未实现事务隔离，写入已落盘）");
            }

            // ------------------------------------------------------------------
            // 5.4 WriteConcern —— w=1 / w="majority"
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 5.4 WriteConcern ---");

            // w=1: 只需主节点确认（默认，延迟低）
            Map<String, Object> cmdW1 = new LinkedHashMap<>();
            cmdW1.put("insert", ACCOUNTS);
            cmdW1.put("ordered", true);
            cmdW1.put("writeConcern", map("w", 1));
            cmdW1.put("$db", DB_NAME);
            Map<String, Object> w1Body = client.getConnection().sendCommand(cmdW1,
                    Arrays.asList(doc("account", "wc_w1", "balance", 0.0)), "documents").body;
            System.out.println("insert with writeConcern w=1: ok=" + w1Body.get("ok") + ", n=" + w1Body.get("n"));

            // w="majority": 需要多数派节点确认，强一致但延迟更高
            Map<String, Object> cmdMajority = new LinkedHashMap<>();
            cmdMajority.put("insert", ACCOUNTS);
            cmdMajority.put("ordered", true);
            cmdMajority.put("writeConcern", map("w", "majority"));
            cmdMajority.put("$db", DB_NAME);
            try {
                Map<String, Object> majorityBody = client.getConnection().sendCommand(cmdMajority,
                        Arrays.asList(doc("account", "wc_majority", "balance", 0.0)), "documents").body;
                System.out.println("insert with writeConcern w=majority: ok=" + majorityBody.get("ok")
                        + ", n=" + majorityBody.get("n"));
            } catch (Exception e) {
                System.out.println("writeConcern w=majority 失败: " + e.getMessage());
            }

            // ------------------------------------------------------------------
            // 5.5 ReadConcern —— local / majority
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 5.5 ReadConcern ---");

            // level=local: 读取本节点最新数据（默认，延迟最低）
            Map<String, Object> readLocal = new LinkedHashMap<>();
            readLocal.put("find", ACCOUNTS);
            readLocal.put("filter", map("account", "alice"));
            readLocal.put("readConcern", map("level", "local"));
            readLocal.put("$db", DB_NAME);
            try {
                Map<String, Object> localResp = client.getConnection().sendCommand(readLocal).body;
                System.out.println("find with readConcern level=local: ok=" + localResp.get("ok"));
            } catch (Exception e) {
                System.out.println("readConcern local 失败: " + e.getMessage());
            }

            // level=majority: 读取已提交的多数派数据，保证一致性
            Map<String, Object> readMajority = new LinkedHashMap<>();
            readMajority.put("find", ACCOUNTS);
            readMajority.put("filter", map("account", "alice"));
            readMajority.put("readConcern", map("level", "majority"));
            readMajority.put("$db", DB_NAME);
            try {
                Map<String, Object> majorityResp = client.getConnection().sendCommand(readMajority).body;
                System.out.println("find with readConcern level=majority: ok=" + majorityResp.get("ok"));
            } catch (Exception e) {
                System.out.println("readConcern majority 失败: " + e.getMessage());
            }

            // ------------------------------------------------------------------
            // 5.6 可重试写入 (Retryable Writes) —— lsid + txnNumber
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 5.6 可重试写入 (lsid + txnNumber) ---");
            System.out.println("原理: 同一会话(ls)对同一事务号(txnNumber)的写入，服务器只执行一次。");
            System.out.println("      网络超时后客户端可安全重发相同命令，不会造成重复扣款。");

            // 生成会话 ID（演示用；标准 MongoDB lsid.id 为 16 字节 Binary）
            byte[] sessionId = new byte[16];
            new Random(42).nextBytes(sessionId);
            Map<String, Object> lsidDoc = new LinkedHashMap<>();
            Map<String, Object> idDoc = new LinkedHashMap<>();
            idDoc.put("$binary", java.util.Base64.getEncoder().encodeToString(sessionId));
            idDoc.put("$type", "00");
            lsidDoc.put("id", idDoc);
            lsidDoc.put("uid", java.util.Base64.getEncoder().encodeToString(new byte[16]));

            // 第一次发送写入命令（带 txnNumber=1）
            Map<String, Object> retryableInsert = new LinkedHashMap<>();
            retryableInsert.put("insert", ACCOUNTS);
            retryableInsert.put("ordered", true);
            retryableInsert.put("lsid", lsidDoc);
            retryableInsert.put("txnNumber", 1);
            retryableInsert.put("$db", DB_NAME);
            try {
                Map<String, Object> firstAttempt = client.getConnection().sendCommand(retryableInsert,
                        Arrays.asList(doc("account", "retry_target", "balance", 10.0, "seq", 1)), "documents").body;
                System.out.println("第一次写入 txnNumber=1: ok=" + firstAttempt.get("ok") + ", n=" + firstAttempt.get("n"));

                // 模拟网络超时后的重试：发送完全相同的命令（同 lsid、同 txnNumber）
                Map<String, Object> secondAttempt = client.getConnection().sendCommand(retryableInsert,
                        Arrays.asList(doc("account", "retry_target", "balance", 10.0, "seq", 1)), "documents").body;
                System.out.println("重试写入 txnNumber=1: ok=" + secondAttempt.get("ok") + ", n=" + secondAttempt.get("n"));

                // 检查是否产生重复文档
                long retryCount = accounts.countDocuments(map("account", "retry_target"));
                System.out.println("retry_target 文档数量 = " + retryCount
                        + (retryCount == 1 ? "（幂等: 未重复）" : "（存在重复，服务器可能未实现幂等去重）"));
            } catch (Exception e) {
                System.out.println("可重试写入示例失败: " + e.getMessage());
            }

            // txnNumber 递增后再次写入（新事务号应产生新文档或更新）
            Map<String, Object> nextTxn = new LinkedHashMap<>();
            nextTxn.put("insert", ACCOUNTS);
            nextTxn.put("ordered", true);
            nextTxn.put("lsid", lsidDoc);
            nextTxn.put("txnNumber", 2);
            nextTxn.put("$db", DB_NAME);
            try {
                Map<String, Object> nextResp = client.getConnection().sendCommand(nextTxn,
                        Arrays.asList(doc("account", "retry_target2", "balance", 20.0)), "documents").body;
                System.out.println("新事务号 txnNumber=2 写入: ok=" + nextResp.get("ok") + ", n=" + nextResp.get("n"));
            } catch (Exception e) {
                System.out.println("txnNumber=2 写入失败: " + e.getMessage());
            }

            // ------------------------------------------------------------------
            // 5.7 事务 + WriteConcern 组合（生产推荐模式）
            // ------------------------------------------------------------------
            System.out.println();
            System.out.println("--- 5.7 事务 + WriteConcern 组合 ---");
            try {
                Map<String, Object> startTxn = new LinkedHashMap<>();
                startTxn.put("startTransaction", 1);
                startTxn.put("writeConcern", map("w", "majority"));
                db.runCommand(startTxn);
                System.out.println("startTransaction + writeConcern w=majority: ok");

                accounts.updateOne(map("account", "alice"), map("$inc", map("balance", 50.0)));
                System.out.println("事务内 alice $inc +50");

                Map<String, Object> commitTxn = new LinkedHashMap<>();
                commitTxn.put("commitTransaction", 1);
                commitTxn.put("writeConcern", map("w", "majority"));
                Map<String, Object> commitResp = db.runCommand(commitTxn);
                System.out.println("commitTransaction + writeConcern w=majority: ok=" + commitResp.get("ok"));
            } catch (Exception e) {
                System.out.println("事务+WriteConcern 组合失败: " + e.getMessage());
            }

            System.out.println();
            System.out.println("最终余额: alice="
                    + accounts.findOne(map("account", "alice")).get("balance")
                    + ", bob=" + accounts.findOne(map("account", "bob")).get("balance"));

            // 清理
            accounts.deleteMany(new LinkedHashMap<>());
            System.out.println("演示集合 " + ACCOUNTS + " 已清空");
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

    /** 独立运行入口：TransactionExample [host] [port] */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 27017;
        run(host, port);
    }
}
