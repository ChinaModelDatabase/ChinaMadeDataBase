package mdb.example;

import mdb.driver.MdbClient;
import mdb.driver.MdbConnection;

import java.util.List;
import java.util.Map;

/**
 * 连接示例：演示如何连接 MDB 服务器、鉴权、健康检查与优雅断开。
 *
 * <p>自研驱动核心类：</p>
 * <ul>
 *   <li>{@link MdbClient} — 客户端入口，管理连接与认证</li>
 *   <li>{@link MdbConnection} — 底层 TCP 连接与 OP_MSG 协议</li>
 * </ul>
 */
public final class ConnectionExample {

    private ConnectionExample() {
    }

    public static void run(String host, int port) throws Exception {
        // ------------------------------------------------------------------
        // 1.1 基本连接
        // ------------------------------------------------------------------
        System.out.println("--- 1.1 基本连接 ---");
        // 构造客户端（此时尚未真正建立 TCP 连接）
        MdbClient client = new MdbClient(host, port);
        // connect() 会完成 TCP 握手，使用带鉴权的构造器时还会执行 SCRAM 认证
        client.connect();
        System.out.println("已连接到 " + host + ":" + port);
        // 连接是否可用：底层 Socket 未关闭即视为已连接
        MdbConnection connection = client.getConnection();
        System.out.println("连接状态: isConnected=" + connection.isConnected());

        // ------------------------------------------------------------------
        // 1.2 Ping —— 最常用的健康检查
        // ------------------------------------------------------------------
        System.out.println();
        System.out.println("--- 1.2 Ping 健康检查 ---");
        // ping 命令不涉及业务数据，服务器返回 ok=1 即表示存活
        boolean alive = client.ping();
        System.out.println("ping 结果: " + (alive ? "服务器存活" : "服务器无响应"));

        // ------------------------------------------------------------------
        // 1.3 服务器状态
        // ------------------------------------------------------------------
        System.out.println();
        System.out.println("--- 1.3 Server Status ---");
        // serverStatus 返回版本、uptime、连接数等运行指标
        Map<String, Object> status = client.getServerStatus();
        printServerStatus(status);

        // ------------------------------------------------------------------
        // 1.4 列出所有数据库
        // ------------------------------------------------------------------
        System.out.println();
        System.out.println("--- 1.4 List Databases ---");
        List<String> databases = client.listDatabases();
        System.out.println("数据库数量: " + databases.size());
        for (String db : databases) {
            System.out.println("  - " + db);
        }

        // ------------------------------------------------------------------
        // 1.5 优雅断开
        // ------------------------------------------------------------------
        System.out.println();
        System.out.println("--- 1.5 优雅断开 ---");
        // 关闭客户端会同时关闭底层 Socket、输入输出流
        client.close();
        System.out.println("已断开连接, isConnected=" + connection.isConnected());

        // ------------------------------------------------------------------
        // 1.6 带认证的连接（SCRAM-SHA-256）
        // ------------------------------------------------------------------
        System.out.println();
        System.out.println("--- 1.6 带认证连接 (SCRAM-SHA-256) ---");
        System.out.println("说明: 需要服务器上存在对应用户，否则会抛出认证异常。");
        try {
            // 5 参构造器：主机、端口、用户名、密码、认证数据库
            // 认证数据库通常为 admin（用户管理）或业务库
            MdbClient authClient = new MdbClient(host, port, "example_user", "example_pass", "admin");
            authClient.connect();
            System.out.println("认证连接成功");
            authClient.close();
        } catch (Exception e) {
            // 演示项目不要求服务器已创建用户，认证失败属于预期情况
            System.out.println("认证连接失败（可能尚未创建用户）: " + e.getMessage());
        }

        // ------------------------------------------------------------------
        // 1.7 连接状态检查 + 重连示意
        // ------------------------------------------------------------------
        System.out.println();
        System.out.println("--- 1.7 连接状态检查 ---");
        MdbClient reconnectClient = new MdbClient(host, port);
        System.out.println("connect() 前 isConnected=" + reconnectClient.getConnection().isConnected());
        reconnectClient.connect();
        System.out.println("connect() 后 isConnected=" + reconnectClient.getConnection().isConnected());
        System.out.println("ping=" + reconnectClient.ping());
        reconnectClient.close();
        System.out.println("close() 后 isConnected=" + reconnectClient.getConnection().isConnected());
    }

    /** 打印 serverStatus 中的关键字段。 */
    private static void printServerStatus(Map<String, Object> status) {
        if (status == null) {
            System.out.println("serverStatus 返回为空");
            return;
        }
        // 自研服务器返回的字段名与 MongoDB 类似，这里打印常用的几个
        Object ok = status.get("ok");
        System.out.println("ok = " + ok);
        for (String key : new String[]{"host", "version", "process", "uptime", "pid"}) {
            if (status.containsKey(key)) {
                System.out.println(key + " = " + status.get(key));
            }
        }
        // 打印前 8 个字段，便于观察完整结构
        int printed = 0;
        System.out.println("完整状态（节选）:");
        for (Map.Entry<String, Object> entry : status.entrySet()) {
            if (printed++ >= 8) {
                System.out.println("  ...");
                break;
            }
            System.out.println("  " + entry.getKey() + " = " + entry.getValue());
        }
    }

    /** 独立运行入口：ConnectionExample [host] [port] */
    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 27017;
        run(host, port);
    }
}
