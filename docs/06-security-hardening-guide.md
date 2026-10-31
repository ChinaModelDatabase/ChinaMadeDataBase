# MDB 安全加固指南

本文档提供 MDB 在麒麟/统信信创环境下的安全部署建议。

## 1. 系统加固

### 1.1 操作系统加固

```bash
# 禁用不必要的服务
systemctl disable avahi-daemon
systemctl disable cups

# 配置 SSH 加固
sed -i 's/#PermitRootLogin yes/PermitRootLogin no/' /etc/ssh/sshd_config
sed -i 's/#PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config
systemctl restart sshd

# 配置密码策略
sed -i 's/PASS_MAX_DAYS.*/PASS_MAX_DAYS   90/' /etc/login.defs
sed -i 's/PASS_MIN_DAYS.*/PASS_MIN_DAYS   7/' /etc/login.defs
sed -i 's/PASS_MIN_LEN.*/PASS_MIN_LEN    12/' /etc/login.defs

# 配置登录超时
echo "TMOUT=600" >> /etc/profile
echo "export TMOUT=600" >> /etc/profile.d/tmout.sh

# 配置审计
systemctl enable auditd
systemctl start auditd
```

### 1.2 文件权限加固

```bash
# MDB 安装目录权限
chown -R mdb:mdb /opt/mdb
chmod 750 /opt/mdb
chmod 640 /opt/mdb/etc/*.conf
chmod 640 /opt/mdb/data/*
chmod 700 /opt/mdb/data/journal

# 日志文件权限
chmod 640 /var/log/mdb/*.log
chown mdb:mdb /var/log/mdb

# 密钥文件权限 (最敏感)
chmod 600 /opt/mdb/etc/keyring.meta
chmod 600 /opt/mdb/etc/tls/*.key
```

### 1.2 网络加固

```bash
# 仅监听必要端口
# MDB 配置: bindIp: 127.0.0.1 (仅本地访问)
# 或绑定内网 IP: bindIp: 10.0.0.100

# 防火墙规则 (firewalld)
firewall-cmd --permanent --add-port=27017/tcp --zone=internal
firewall-cmd --reload

# 或 iptables
iptables -A INPUT -s 10.0.0.0/8 -p tcp --dport 27017 -j ACCEPT
iptables -A INPUT -p tcp --dport 27017 -j DROP
```

## 2. MDB 配置加固

### 2.1 最小化配置

```yaml
# /opt/mdb/etc/mdb.conf
net:
  bindIp: 127.0.0.1
  port: 27017
  tls:
    mode: requireTLS
    certificateKeyFile: /opt/mdb/etc/tls/server.pem
    CAFile: /opt/mdb/etc/tls/ca.pem
    useGmssl: true  # 启用国密

security:
  authentication: SCRAM-SHA-256
  scramSha256Iterations: 15000

storage:
  dbPath: /opt/mdb/data
  mdb:
    memtableSizeMB: 256
    compactionThreads: 4
    checksum: read

systemLog:
  destination: file
  logPath: /opt/mdb/logs/mdb.log
  logLevel: info
  auditLog:
    enabled: true
```

### 2.2 用户管理

```javascript
// 连接 MDB
mdbsh --host 127.0.0.1 --port 27017

// 创建管理员用户
use admin
db.createUser({
  user: "mdbAdmin",
  pwd: "StrongP@ssw0rd!2024",
  roles: [
    { role: "root", db: "admin" }
  ]
})

// 创建应用用户 (最小权限)
use myapp
db.createUser({
  user: "appUser",
  pwd: "AppP@ssw0rd!2024",
  roles: [
    { role: "readWrite", db: "myapp" }
  ]
})

// 创建只读用户
db.createUser({
  user: "readOnly",
  pwd: "ReadP@ssw0rd!2024",
  roles: [
    { role: "read", db: "myapp" }
  ]
})
```

### 2.3 审计配置

```javascript
// 启用审计日志
db.adminCommand({
  setParameter: 1,
  auditAuthorizationSuccess: true
})

// 查看审计日志
db.adminCommand({ getLog: "audit" })
```

## 3. TLS/TLCP 配置

### 3.1 生成国密证书

```bash
# 生成 SM2 密钥对
gm-cert-gen -key sm2 -out server.key
gm-cert-gen -sign -key server.key -out server.crt -subject "CN=mdb-server"

# 生成 CA 证书
gm-cert-gen -key sm2 -out ca.key
gm-cert-gen -sign -key ca.key -out ca.crt -subject "CN=MDB-CA"

# 合并为 PEM
cat server.key server.crt > server.pem
```

### 3.2 配置 TLS

```yaml
net:
  tls:
    mode: requireTLS
    certificateKeyFile: /opt/mdb/etc/tls/server.pem
    CAFile: /opt/mdb/etc/tls/ca.pem
    useGmssl: true
```

### 3.3 客户端连接

```python
# Python 连接 (TLS)
from mdb_native import MdbClient
client = MdbClient("localhost", 27017, tls=True, tls_ca="/opt/mdb/etc/tls/ca.pem")
client.connect()
```

## 4. 备份策略

### 4.1 全量备份

```bash
#!/bin/bash
# /opt/mdb/scripts/backup.sh

BACKUP_DIR="/opt/mdb/backups"
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_PATH="$BACKUP_DIR/backup_$DATE"

# 创建备份目录
mkdir -p "$BACKUP_PATH"

# 锁定数据库 (fsync)
mdb-admin --host 127.0.0.1 --port 27017 fsync

# 复制数据文件
cp -r /opt/mdb/data/* "$BACKUP_PATH/"

# 记录备份信息
echo "Backup completed: $DATE" > "$BACKUP_PATH/backup_info.txt"
echo "MDB version: $(mdb-server --version)" >> "$BACKUP_PATH/backup_info.txt"

# 清理旧备份 (保留 7 天)
find "$BACKUP_DIR" -name "backup_*" -mtime +7 -exec rm -rf {} \;

echo "Backup saved to: $BACKUP_PATH"
```

### 4.2 定时备份

```cron
# /etc/crontab
0 2 * * * root /opt/mdb/scripts/backup.sh >> /var/log/mdb/backup.log 2>&1
```

## 5. 监控告警

### 5.1 Prometheus 指标

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'mdb'
    static_configs:
      - targets: ['localhost:9216']
    metrics_path: '/metrics'
```

### 5.2 关键告警规则

```yaml
# mdb_alerts.yml
groups:
  - name: mdb
    rules:
      - alert: MdbDown
        expr: up{job="mdb"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "MDB instance is down"

      - alert: MdbHighConnections
        expr: mdb_connections_open > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "MDB connection count is high"

      - alert: MdbSlowQueries
        expr: rate(mdb_commands_total{slow="true"}[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "MDB slow query rate is high"
```

## 6. 应急响应

### 6.1 数据泄露应急

1. 立即隔离受影响节点
2. 保留现场证据 (日志、网络流量)
3. 通知安全团队
4. 评估影响范围
5. 修复漏洞
6. 恢复服务
7. 编写事件报告

### 6.2 数据损坏应急

1. 停止写入
2. 使用最近备份恢复
3. 应用 WAL 日志到备份点
4. 验证数据完整性
5. 恢复服务

### 6.3 联系方式

- 安全应急: security@mdb-project.com
- 技术支持: support@mdb-project.com
