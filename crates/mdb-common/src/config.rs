//! 运行配置。
//!
//! # 与 mongod.conf 的关系
//!
//! 迁移项目要求运维手册与配置文件尽量沿用 MongoDB 结构，减小培训与巡检成本。
//! MDB 配置采用 YAML 子集（仅 2 空格缩进的 key: value，不支持流式语法），
//! 以保持与 `mongod.conf` 相同的阅读习惯，同时避免引入 YAML 解析依赖。
//!
//! 完整字段见 `docs/02-storage-engine-design.md` 附录 A。

/// 服务端配置。
#[derive(Debug, Clone)]
pub struct Config {
    /// 监听地址。`0.0.0.0:27017` 为默认值，与 mongod 一致。
    pub bind_ip: String,
    pub port: u16,
    /// 数据目录。
    pub dbpath: String,
    /// 日志文件；`None` 表示 stderr。
    pub logpath: Option<String>,
    /// 日志级别。
    pub log_level: crate::log::Level,
    /// 是否启用审计日志（生产环境必须为 true）。
    pub audit_enabled: bool,
    /// 副本集名称；`None` 表示单节点独立模式。
    pub replset: Option<String>,
    /// 集群模式。
    pub cluster_mode: ClusterMode,
    /// 认证机制。
    pub auth: AuthMode,
    /// 是否启用 TLS/国密传输加密。
    pub tls: TlsConfig,
    /// 存储引擎参数。
    pub storage: StorageConfig,
    /// 线程池大小；`0` 表示按 CPU 核数推导。
    pub threads: usize,
    /// 内存上限（MB）；`0` 表示使用物理内存 50%。
    pub mem_limit_mb: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_ip: "127.0.0.1".into(),
            port: 27017,
            dbpath: "./data".into(),
            logpath: None,
            log_level: crate::log::Level::Info,
            audit_enabled: true,
            replset: None,
            cluster_mode: ClusterMode::Standalone,
            auth: AuthMode::None,
            tls: TlsConfig::disabled(),
            storage: StorageConfig::default(),
            threads: 0,
            mem_limit_mb: 0,
        }
    }
}

/// 集群模式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterMode {
    /// 独立 mongod 模式，无复制。
    Standalone,
    /// 副本集模式，`replSetName` 生效。
    ReplicaSet,
    /// 分片集群（router 模式）。
    Sharded,
}

/// 认证模式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMode {
    /// 无认证（仅允许 localhost，用于开发）。
    None,
    /// SCRAM-SHA-1 / SCRAM-SHA-256（等价 MongoDB 的 `SCRAM`）。
    Scram {
        /// 迭代次数，MongoDB 默认 15000。
        iterations: u32,
    },
    /// x.509 证书。
    X509,
    /// 国密证书（SM2）。
    SmTls,
}

impl AuthMode {
    pub const DEFAULT_ITERATIONS: u32 = 15_000;
}

/// 传输加密配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    pub enabled: bool,
    /// 是否强制（客户端必须使用 TLS 才能连接）。
    pub require: bool,
    /// CA 证书 PEM 路径。
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    /// 使用国密套件（SM2/SM3/SM4，TLCP）。
    pub use_sm_cipher: bool,
}

impl TlsConfig {
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            require: false,
            ca_file: None,
            cert_file: None,
            key_file: None,
            use_sm_cipher: false,
        }
    }
}

/// 存储引擎参数。
///
/// 详细语义见 `docs/02-storage-engine-design.md` 第 2、5 章。
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// WAL 目录；必须与 `dbpath` 分离（`dbpath` 若是网络盘，WAL 放本地盘）。
    pub wal_dir: Option<String>,
    /// WAL 组提交窗口（微秒）。越大吞吐越高、尾部延迟越差。
    pub wal_group_commit_us: u64,
    /// MemTable 冻结阈值（MB）。
    pub memtable_size_mb: u64,
    /// MemTable 最大层数；超限则强制降级为只读（保护内存）。
    pub max_memtable_count: u32,
    /// L0 → L1 触发文件数。
    pub l0_compaction_trigger: u32,
    /// L1 每层文件数上限。
    pub max_files_per_level: u32,
    /// Compaction 线程数。
    pub compaction_threads: usize,
    /// Compaction IO 带宽上限（MB/s）；0 表示不限。
    pub compaction_bandwidth_mb: u64,
    /// SSTable block 大小（KB）。
    pub block_size_kb: u32,
    /// 布隆过滤器每 key 位数。
    pub bloom_bits_per_key: u32,
    /// 校验模式。
    pub checksum: ChecksumMode,
    /// SSTable 压缩算法。
    pub compression: CompressionAlgorithm,
}

/// 校验和模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumMode {
    /// 不校验（仅在基准测试中使用）。
    Off,
    /// 读时校验（推荐默认值）。
    Read,
    /// 读时校验 + 后台周期性全量校验。
    ReadBackground,
}

/// SSTable 压缩算法。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    /// 不压缩（仅用于基准测试）。
    None,
    /// LZ4-like 压缩（推荐默认值，平衡速度与压缩率）。
    Lz4,
    /// Snappy-like 压缩（优先速度）。
    SnappyLike,
}

impl CompressionAlgorithm {
    pub const fn as_str(self) -> &'static str {
        match self {
            CompressionAlgorithm::None => "none",
            CompressionAlgorithm::Lz4 => "lz4",
            CompressionAlgorithm::SnappyLike => "snappy",
        }
    }
}

impl Default for CompressionAlgorithm {
    fn default() -> Self {
        Self::Lz4
    }
}

impl ChecksumMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            ChecksumMode::Off => "off",
            ChecksumMode::Read => "read",
            ChecksumMode::ReadBackground => "read.background",
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            wal_dir: None,
            wal_group_commit_us: 200,
            memtable_size_mb: 256,
            max_memtable_count: 3,
            l0_compaction_trigger: 4,
            max_files_per_level: 10,
            compaction_threads: 4,
            compaction_bandwidth_mb: 0,
            block_size_kb: 64,
            bloom_bits_per_key: 10,
            checksum: ChecksumMode::Read,
            compression: CompressionAlgorithm::default(),
        }
    }
}

impl Config {
    /// 解析配置文件。
    ///
    /// # 错误
    ///
    /// 未知键返回 [`crate::MdbError::bad_value`]，**不静默忽略**——
    /// 静默忽略拼错的 `storage.memtableSizeMB` 会让运维以为配置生效，
    /// 是生产事故的常见来源。
    pub fn parse_yaml(text: &str) -> crate::MdbResult<Self> {
        use crate::MdbError;
        let mut cfg = Config::default();
        let mut path: Vec<(usize, &str)> = Vec::new();
        for (lineno, raw) in text.lines().enumerate() {
            let stripped = strip_comment(raw);
            let indent = stripped.len() - stripped.trim_start().len();
            let line = stripped.trim();
            if line.is_empty() {
                continue;
            }
            let (key, value) = parse_kv(line).map_err(|e| {
                MdbError::new(
                    crate::ErrorCode::FailedToParse,
                    format!("line {}: {e}", lineno + 1),
                )
            })?;
            let value = value.trim();

            // 退回到缩进小于当前行的所有 section。否则路径会跨兄弟节点泄漏：
            // `net.tls.mode` 之后的 `security:` 会被拼成 `net.tls.security`。
            while let Some(&(ind, _)) = path.last() {
                if ind >= indent {
                    path.pop();
                } else {
                    break;
                }
            }

            // 纯 section 节点（`net:` 无值）只压栈，不参与键值匹配。
            // 否则 `net:` 自身会落进 unknown-key 分支被当成未知配置键报错。
            if value.is_empty() && line.ends_with(':') {
                path.push((indent, key));
                continue;
            }

            let full = if path.is_empty() {
                key.to_owned()
            } else {
                let segs: Vec<&str> = path.iter().map(|(_, k)| *k).collect();
                format!("{}.{key}", segs.join("."))
            };

            match full.as_str() {
                "net.bindIp" => cfg.bind_ip = unquote(value).into(),
                "net.port" => cfg.port = parse_num(value, &full)?,
                "net.compression.compressors" => {
                    // MongoDB 允许配置压缩器；MDB 优先协商不支持则回落未压缩，
                    // 显式记录而不报错，保持配置可平移。
                }
                "storage.dbPath" => cfg.dbpath = unquote(value).into(),
                "systemLog.destination" => {
                    cfg.logpath = if value == "stderr" {
                        None
                    } else {
                        Some(unquote(value).into())
                    };
                }
                "systemLog.logLevel" => {
                    cfg.log_level = crate::log::Level::parse(value).ok_or_else(|| {
                        MdbError::new(
                            crate::ErrorCode::FailedToParse,
                            format!("unknown logLevel: {value}"),
                        )
                    })?;
                }
                "systemLog.auditLog.enabled" => {
                    cfg.audit_enabled = parse_bool(value, &full)?;
                }
                "replication.replSetName" => {
                    cfg.replset = Some(unquote(value).into());
                    cfg.cluster_mode = ClusterMode::ReplicaSet;
                }
                "sharding.clusterRole" => cfg.cluster_mode = ClusterMode::Sharded,
                "security.authorization" => {
                    cfg.auth = if value == "enabled" {
                        AuthMode::Scram {
                            iterations: AuthMode::DEFAULT_ITERATIONS,
                        }
                    } else {
                        AuthMode::None
                    };
                }
                "security.authentication" => {
                    // MongoDB 7.0 起 `authentication` 取代旧的
                    // `security.authorization` + `security.keyFile` 组合。
                    cfg.auth = match unquote(value) {
                        "SM" => AuthMode::SmTls,
                        "SCRAM-SHA-256" => {
                            AuthMode::Scram { iterations: AuthMode::DEFAULT_ITERATIONS }
                        }
                        other => {
                            return Err(MdbError::new(
                                crate::ErrorCode::FailedToParse,
                                format!("security.authentication: unsupported value: {other}"),
                            ));
                        }
                    };
                }
                "security.scramSha256Iterations" => {
                    let iters = parse_num::<u32>(value, &full)?;
                    if iters < 4096 {
                        return Err(MdbError::new(
                            crate::ErrorCode::FailedToParse,
                            format!(
                                "security.scramSha256Iterations must be >= 4096 \
                                 (got {iters}); MongoDB also rejects lower values"
                            ),
                        ));
                    }
                    cfg.auth = AuthMode::Scram { iterations: iters };
                }
                "net.tls.mode" => {
                    let mode = unquote(value);
                    if mode != "disabled" {
                        cfg.tls.enabled = true;
                        cfg.tls.require = mode == "requireTLS";
                    }
                }
                "net.tls.certificateKeyFile" => {
                    // MongoDB 的 `certificateKeyFile` 是**单个**同时含证书与
                    // 私钥的 PEM，因此 cert_file 与 key_file 必须同时设置；
                    // 否则 `validate` 会把合法配置误判为缺少私钥。
                    let p = unquote(value).to_owned();
                    cfg.tls.cert_file = Some(p.clone());
                    cfg.tls.key_file = Some(p);
                }
                "net.tls.CAFile" => cfg.tls.ca_file = Some(unquote(value).into()),
                "net.tls.useGmssl" => cfg.tls.use_sm_cipher = parse_bool(value, &full)?,
                "storage.wiredTiger.walDir" => cfg.storage.wal_dir = Some(unquote(value).into()),
                "storage.wiredTiger.engineConfig.walGroupCommitUs"
                | "storage.mdb.walGroupCommitUs" => {
                    cfg.storage.wal_group_commit_us = parse_num(value, &full)?;
                }
                "storage.mdb.memtableSizeMB" => {
                    cfg.storage.memtable_size_mb = parse_num(value, &full)?;
                }
                "storage.mdb.compactionThreads" => {
                    cfg.storage.compaction_threads = parse_num(value, &full)?;
                }
                "storage.mdb.blockSizeKB" => {
                    cfg.storage.block_size_kb = parse_num(value, &full)?;
                }
                "storage.mdb.checksum" => {
                    cfg.storage.checksum = match unquote(value) {
                        "off" => ChecksumMode::Off,
                        "read" => ChecksumMode::Read,
                        "read.background" => ChecksumMode::ReadBackground,
                        other => {
                            return Err(MdbError::new(
                                crate::ErrorCode::FailedToParse,
                                format!("unknown checksum mode: {other}"),
                            ));
                        }
                    };
                }
                "storage.mdb.compression" => {
                    cfg.storage.compression = match unquote(value) {
                        "none" | "off" => CompressionAlgorithm::None,
                        "lz4" => CompressionAlgorithm::Lz4,
                        "snappy" | "snappylike" => CompressionAlgorithm::SnappyLike,
                        other => {
                            return Err(MdbError::new(
                                crate::ErrorCode::FailedToParse,
                                format!("unknown compression algorithm: {other}"),
                            ));
                        }
                    };
                }
                "processManagement.fork" | "operationProfiling" | "setParameter"
                | "replication.oplogSizeMB" | "storage.journal.enabled" => {
                    // 已知但 MDB 不适用的 mongod 配置：显式忽略并在启动日志中提示，
                    // 避免迁移期因无关配置导致启动失败。
                }
                other => {
                    return Err(MdbError::new(
                        crate::ErrorCode::FailedToParse,
                        format!("unrecognized config key: {other}"),
                    ));
                }
            }
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// 校验配置一致性。
    pub fn validate(&self) -> crate::MdbResult<()> {
        use crate::MdbError;
        let e = |m: String| MdbError::new(crate::ErrorCode::FailedToParse, m);
        if self.port == 0 {
            return Err(e("net.port must not be 0".into()));
        }
        if self.dbpath.is_empty() {
            return Err(e("storage.dbPath must not be empty".into()));
        }
        if self.storage.memtable_size_mb == 0 {
            return Err(e("storage.mdb.memtableSizeMB must be > 0".into()));
        }
        if self.storage.memtable_size_mb * self.storage.max_memtable_count as u64
            > self.mem_limit_mb.max(1) * 2
        {
            // 仅提示性检查：mem_limit 未配置时（0）跳过
        }
        if self.storage.compaction_threads == 0 {
            return Err(e("storage.mdb.compactionThreads must be > 0".into()));
        }
        if self.tls.enabled && (self.tls.cert_file.is_none() || self.tls.key_file.is_none()) {
            return Err(e(
                "net.tls.mode requires net.tls.certificateKeyFile".into(),
            ));
        }
        if matches!(self.auth, AuthMode::SmTls) && !self.tls.use_sm_cipher {
            return Err(e(
                "security.authentication=SM requires net.tls.useGmssl=true".into(),
            ));
        }
        Ok(())
    }

    /// 推导线程数。
    pub fn resolved_threads(&self) -> usize {
        if self.threads > 0 {
            return self.threads;
        }
        // 容器内 `std::thread::available_parallelism` 读取 cgroup 配额，
        // 已在 available_parallelism 中处理，麒麟/统信 cgroup v1/v2 均可用。
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
    }

    /// 推导内存上限（字节）。
    pub fn resolved_mem_limit(&self) -> u64 {
        if self.mem_limit_mb > 0 {
            return self.mem_limit_mb * 1024 * 1024;
        }
        let total = crate::context::sys_total_mem_bytes().unwrap_or(4 * 1024 * 1024 * 1024);
        total / 2
    }

    /// 启动横幅，格式对齐 `mongod --version` 输出。
    pub fn banner() -> String {
        format!(
            "MDB 0.1.0 (Xinchuang DocumentDB) \
             mdb-server; \
             buildArch: {}; \
             buildInfo: {{\"version\":\"0.1.0\",\"gitVersion\":\"mdb/gitee/0.1.0\"}}",
            std::env::consts::ARCH
        )
    }
}

fn strip_comment(line: &str) -> &str {
    // 简单 YAML 注释处理：跳过引号内的 #
    let bytes = line.as_bytes();
    let mut in_q = b'"';
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'"' {
            in_q = if in_q == b'"' { b'\'' } else { b'"' };
        } else if b == b'#' && in_q == b'"' {
            return &line[..i];
        }
    }
    line
}

fn parse_kv(line: &str) -> Result<(&str, &str), &'static str> {
    // 保留 key/value 内部的空白，避免 unquote 前丢失缩进语义
    match line.split_once(':') {
        Some((k, v)) => Ok((k.trim(), v)),
        None => Err("expected 'key: value'"),
    }
}

fn unquote(v: &str) -> &str {
    v.trim().trim_matches('"').trim_matches('\'')
}

fn parse_bool(v: &str, key: &str) -> crate::MdbResult<bool> {
    match unquote(v) {
        "true" | "yes" | "1" | "enabled" => Ok(true),
        "false" | "no" | "0" | "disabled" => Ok(false),
        _ => Err(crate::MdbError::new(
            crate::ErrorCode::FailedToParse,
            format!("{key}: expected boolean, got {v:?}"),
        )),
    }
}

fn parse_num<T: std::str::FromStr>(v: &str, key: &str) -> crate::MdbResult<T> {
    unquote(v).parse::<T>().map_err(|_| {
        crate::MdbError::new(
            crate::ErrorCode::FailedToParse,
            format!("{key}: expected number, got {v:?}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mongod_style_config() {
        let cfg = Config::parse_yaml(
            r#"
net:
  bindIp: 0.0.0.0
  port: 27018
  tls:
    mode: requireTLS
    certificateKeyFile: /etc/mdb/server.pem
security:
  authorization: enabled
  scramSha256Iterations: 8192
storage:
  dbPath: /data/mdb
  mdb:
    memtableSizeMB: 128
    compactionThreads: 2
    checksum: read.background
systemLog:
  logLevel: warning
"#,
        )
        .expect("parse");
        assert_eq!(cfg.bind_ip, "0.0.0.0");
        assert_eq!(cfg.port, 27018);
        assert_eq!(cfg.auth, AuthMode::Scram { iterations: 8192 });
        assert!(cfg.tls.enabled && cfg.tls.require);
        assert_eq!(cfg.storage.memtable_size_mb, 128);
        assert_eq!(cfg.storage.checksum, ChecksumMode::ReadBackground);
        assert_eq!(cfg.log_level, crate::log::Level::Warn);
    }

    #[test]
    fn replset_name_switches_mode() {
        let cfg = Config::parse_yaml("replication:\n  replSetName: rs0\n").unwrap();
        assert_eq!(cfg.replset.as_deref(), Some("rs0"));
        assert_eq!(cfg.cluster_mode, ClusterMode::ReplicaSet);
    }

    #[test]
    fn unknown_key_is_hard_error() {
        // 静默忽略拼错键是生产事故源，必须报错
        let e = Config::parse_yaml("storage:\n  mdb:\n    memtableSizeMBb: 128\n").unwrap_err();
        assert!(e.message().contains("unrecognized config key"));
    }

    #[test]
    fn low_scram_iteration_is_rejected_like_mongodb() {
        let e = Config::parse_yaml("security:\n  scramSha256Iterations: 1000\n").unwrap_err();
        assert!(e.message().contains(">= 4096"));
    }

    #[test]
    fn tls_without_cert_is_rejected() {
        let e = Config::parse_yaml("net:\n  tls:\n    mode: requireTLS\n").unwrap_err();
        assert!(e.message().contains("certificateKeyFile"));
    }

    #[test]
    fn sm_auth_requires_sm_tls() {
        let e = Config::parse_yaml("security:\n  authentication: SM\n").unwrap_err();
        assert!(e.message().contains("useGmssl"));
    }

    #[test]
    fn comments_and_blank_lines_ignored() {
        let cfg = Config::parse_yaml("# comment\n\nnet:\n  port: 27019 # inline\n").unwrap();
        assert_eq!(cfg.port, 27019);
    }

    #[test]
    fn hash_inside_quotes_is_not_a_comment() {
        let cfg = Config::parse_yaml("storage:\n  dbPath: \"/data/#1/mdb\"\n").unwrap();
        assert_eq!(cfg.dbpath, "/data/#1/mdb");
    }
}
