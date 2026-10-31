package mdb.example;

import java.util.ArrayList;
import java.util.List;

/**
 * MDB Java 示例主入口。
 *
 * <p>按顺序运行全部示例：连接、CRUD、索引、聚合、事务、GridFS、Change Stream、管理操作。</p>
 *
 * <p>用法：</p>
 * <pre>
 *   mvn compile exec:java
 *   mvn compile exec:java -Dexec.args="localhost 27017"
 *   mvn compile exec:java -Dexec.args="localhost 27017 crud"
 * </pre>
 *
 * <p>也可以单独运行某个示例类的 main 方法。</p>
 */
public final class Main {

    private static final String SEPARATOR = "============================================================";

    /** 示例任务接口：允许抛出异常，由 Main 统一捕获并打印。 */
    @FunctionalInterface
    interface ExampleTask {
        void run() throws Exception;
    }

    private Main() {
    }

    public static void main(String[] args) {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 27017;
        String only = args.length > 2 ? args[2] : null;

        System.out.println(SEPARATOR);
        System.out.println("  MDB Java Example —— 自研驱动 mdb-native 功能演示");
        System.out.println("  目标服务器: " + host + ":" + port);
        System.out.println(SEPARATOR);

        // 定义全部示例：名称 -> 任务
        List<String[]> meta = new ArrayList<>();
        meta.add(new String[]{"connection", "连接示例 - Connection"});
        meta.add(new String[]{"crud", "CRUD 操作 - Crud"});
        meta.add(new String[]{"index", "索引管理 - Index"});
        meta.add(new String[]{"aggregation", "聚合管道 - Aggregation"});
        meta.add(new String[]{"transaction", "事务 - Transaction"});
        meta.add(new String[]{"gridfs", "GridFS 文件存储 - GridFS"});
        meta.add(new String[]{"changestream", "Change Stream - ChangeStream"});
        meta.add(new String[]{"admin", "管理操作 - Admin"});

        List<ExampleTask> tasks = new ArrayList<>();
        tasks.add(() -> ConnectionExample.run(host, port));
        tasks.add(() -> CrudExample.run(host, port));
        tasks.add(() -> IndexExample.run(host, port));
        tasks.add(() -> AggregationExample.run(host, port));
        tasks.add(() -> TransactionExample.run(host, port));
        tasks.add(() -> GridFSExample.run(host, port));
        tasks.add(() -> ChangeStreamExample.run(host, port));
        tasks.add(() -> AdminExample.run(host, port));

        int total = meta.size();
        int ok = 0;
        int skipped = 0;
        int failed = 0;

        for (int i = 0; i < total; i++) {
            String key = meta.get(i)[0];
            String title = meta.get(i)[1];

            if (only != null && !only.equalsIgnoreCase(key)) {
                skipped++;
                continue;
            }

            System.out.println();
            System.out.println(SEPARATOR);
            System.out.printf("  [%d/%d] %s%n", i + 1, total, title);
            System.out.println(SEPARATOR);

            long start = System.currentTimeMillis();
            try {
                tasks.get(i).run();
                long ms = System.currentTimeMillis() - start;
                System.out.println();
                System.out.println("  >> " + title + " 完成，耗时 " + ms + " ms");
                ok++;
            } catch (Exception e) {
                long ms = System.currentTimeMillis() - start;
                System.out.println();
                System.out.println("  >> " + title + " 失败 (" + ms + " ms)");
                System.out.println("  错误: " + e.getMessage());
                e.printStackTrace(System.out);
                failed++;
            }
        }

        System.out.println();
        System.out.println(SEPARATOR);
        System.out.printf("  运行结束: 成功 %d, 失败 %d, 跳过 %d%n", ok, failed, skipped);
        System.out.println(SEPARATOR);
    }
}
