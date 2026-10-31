//! 持久化存储层：把 `mdb-server` 的内存 `MemStore` 接到 LSM 存储引擎上。
//!
//! 设计要点：
//!
//! * 每个集合一个列族（column family），命名 `coll.{db}.{coll}`。列族是存储引擎
//!   独立的 WAL + SST 单元，集合之间的写入互不干扰，也便于按集合做
//!   Compaction 与加密。
//! * 元数据存在系统列族 `mdb.meta`：数据库列表、集合列表、索引定义、
//!   用户记录、`_id` 计数器。重启时从这里重建内存目录。
//! * 写入先落 WAL 再返回，因此 `insert` 返回成功后数据已持久化。
//! * 索引在内存中维护，启动时通过重放集合数据重建，避免把索引也写进 LSM
//!   增加写放大。

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use mdb_common::config::StorageConfig;
use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_doc::{Bson, Document};
use mdb_storage::Engine;

use crate::DocExt;

/// 系统列族名，存放所有元数据。
pub const META_CF: &str = "mdb.meta";

/// 索引规格（持久化形态）。字段顺序即复合索引的键顺序。
#[derive(Debug, Clone)]
pub struct StoredIndexSpec {
    pub name: String,
    pub fields: Vec<String>,
    pub unique: bool,
    pub sparse: bool,
    pub expire_after_seconds: Option<i64>,
    pub partial_filter: Option<Document>,
}

/// 集合的持久化元数据。
#[derive(Debug, Clone, Default)]
pub struct StoredCollectionMeta {
    pub next_record_id: i64,
    pub indexes: Vec<StoredIndexSpec>,
}

/// 索引规格在内存中的可变视图，索引管理器直接消费。
#[derive(Debug, Clone)]
pub struct LiveIndexSpec {
    pub name: String,
    pub fields: Vec<String>,
    pub unique: bool,
}

/// 从持久化文档还原出的用户凭据。
#[derive(Debug, Clone)]
pub struct ParsedUser {
    pub username: String,
    pub database: String,
    pub mechanism: String,
    pub iteration_count: u32,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub roles: Vec<(String, String)>,
}

/// 把 `StoredKey` 编成可持久化的 BSON 子文档。
pub fn credential_doc(
    mechanism: &str,
    iteration_count: u32,
    salt: &[u8],
    stored_key: &[u8],
    server_key: &[u8],
) -> Document {
    let mut c = Document::new();
    c.insert("mechanism", Bson::String(mechanism.to_owned()));
    c.insert("iterations", Bson::Int64(iteration_count as i64));
    c.insert("salt", Bson::Binary(mdb_doc::binary::Binary::generic(salt.to_vec())));
    c.insert("storedKey", Bson::Binary(mdb_doc::binary::Binary::generic(stored_key.to_vec())));
    c.insert("serverKey", Bson::Binary(mdb_doc::binary::Binary::generic(server_key.to_vec())));
    c
}

/// 把 `Bson` 里的二进制字段取出字节内容。
fn binary_field(v: &Bson) -> Option<Vec<u8>> {
    match v {
        Bson::Binary(b) => Some(b.bytes.clone()),
        Bson::String(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

/// 组装一条完整用户记录的持久化文档。
pub fn user_doc(
    username: &str,
    database: &str,
    credential: Document,
    roles: &[(String, String)],
) -> Document {
    let mut doc = Document::new();
    doc.insert("_id", Bson::String(format!("{username}@{database}")));
    doc.insert("user", Bson::String(username.to_owned()));
    doc.insert("db", Bson::String(database.to_owned()));
    doc.insert("credential", Bson::Document(credential));
    let role_docs: Vec<Bson> = roles
        .iter()
        .map(|(r, d)| {
            let mut rd = Document::new();
            rd.insert("role", Bson::String(r.clone()));
            rd.insert("db", Bson::String(d.clone()));
            Bson::Document(rd)
        })
        .collect();
    doc.insert("roles", Bson::Array(role_docs));
    doc
}

/// 整个服务端的持久化句柄。
///
/// `ServerState` 持有一个 `Arc<Catalog>`；所有集合共享同一个 `Engine`。
#[derive(Debug)]
pub struct Catalog {
    engine: Engine,
    /// 集合元数据：`"{db}.{coll}" -> meta`，加写锁保证 `next_record_id` 原子自增。
    metas: Mutex<BTreeMap<String, StoredCollectionMeta>>,
    /// 数据目录路径（用于 TDE 密钥环等）。
    db_path: std::path::PathBuf,
}

impl Catalog {
    /// 打开（或创建）位于 `dbpath` 的存储引擎，并加载元数据。
    pub fn open(dbpath: &Path, config: StorageConfig) -> MdbResult<Self> {
        let engine = Engine::open(dbpath, config)?;
        let catalog = Self {
            engine,
            metas: Mutex::new(BTreeMap::new()),
            db_path: dbpath.to_path_buf(),
        };
        catalog.ensure_meta_cf()?;
        catalog.load_metas()?;
        Ok(catalog)
    }

    /// 返回数据目录路径。
    pub fn db_path(&self) -> &std::path::Path {
        &self.db_path
    }

    fn ensure_meta_cf(&self) -> MdbResult<()> {
        // `Engine::create_cf` 对已存在的列族返回错误，这里容忍该错误，
        // 因为重启时列族必然已存在。
        match self.engine.create_cf(META_CF) {
            Ok(()) => Ok(()),
            Err(e) if e.message().contains("already exists") => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn meta_key(db: &str, coll: &str) -> String {
        format!("{db}.{coll}")
    }

    /// 集合在存储引擎中的列族名。
    fn cf_name(db: &str, coll: &str) -> String {
        // 列族名会作为目录名（`colcf/cf_{id}`）与 manifest 键，因此必须做字符白名单处理，
        // 避免 db/coll 名里的 `/`、`..` 等字符逃逸出 dbpath。
        format!(
            "coll.{}.{}",
            sanitize_cf_component(db),
            sanitize_cf_component(coll)
        )
    }

    fn load_metas(&self) -> MdbResult<()> {
        let entries = self
            .engine
            .range_scan(META_CF, b"collmeta.", &prefix_end(b"collmeta."))?;
        let mut metas = self.metas.lock().map_err(lock_err)?;
        for (_, value) in entries {
            let Ok(doc) = mdb_doc::codec::from_bytes(&value) else {
                continue;
            };
            let Some(ns) = doc.get_str("ns") else {
                continue;
            };
            metas.insert(ns.to_owned(), decode_meta(&doc));
        }
        Ok(())
    }

    /// 列出所有已持久化的命名空间（`"{db}.{coll}"`）。
    pub fn list_namespaces(&self) -> Vec<String> {
        self.metas
            .lock()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// 列出某个数据库下的全部集合名。
    ///
    /// 直接扫元数据列族而不是读内存 `metas`：`metas` 只反映本进程内的
    /// `create_collection` 调用，重启后必须从磁盘重建才完整。
    ///
    /// 元数据键是 `"{db}.{coll}"`。数据库名可含 `.`，故用"首段等于 db"判定：
    /// 对 `db = "a"`，`a.b.c` 属于库 `a` 的集合 `b.c`。
    pub fn list_collections(&self, db: &str) -> MdbResult<Vec<String>> {
        let entries = self
            .engine
            .range_scan(META_CF, b"collmeta.", &prefix_end(b"collmeta."))?;
        let mut out = Vec::new();
        for (_, value) in entries {
            let Ok(ns) = mdb_doc::codec::from_bytes(&value) else {
                continue;
            };
            let Some(ns) = ns.get("ns").and_then(|v| v.as_str()) else {
                continue;
            };
            match ns.split_once('.') {
                Some((d, coll)) if d == db && !coll.is_empty() => out.push(coll.to_owned()),
                _ => {}
            }
        }
        Ok(out)
    }

    /// 判断集合是否已存在于磁盘。
    pub fn collection_exists(&self, db: &str, coll: &str) -> bool {
        self.metas
            .lock()
            .map(|m| m.contains_key(&Self::meta_key(db, coll)))
            .unwrap_or(false)
    }

    /// 创建集合（幂等）。返回 false 表示集合此前已存在。
    pub fn create_collection(&self, db: &str, coll: &str) -> MdbResult<bool> {
        let ns = Self::meta_key(db, coll);
        {
            let metas = self.metas.lock().map_err(lock_err)?;
            if metas.contains_key(&ns) {
                return Ok(false);
            }
        }
        let cf = Self::cf_name(db, coll);
        match self.engine.create_cf(&cf) {
            Ok(()) => {}
            Err(e) if e.message().contains("already exists") => {}
            Err(e) => return Err(e),
        }
        let meta = StoredCollectionMeta {
            next_record_id: 1,
            ..Default::default()
        };
        self.write_meta(&ns, &meta)?;
        self.metas.lock().map_err(lock_err)?.insert(ns, meta);
        Ok(true)
    }

    /// 删除集合及其列族。元数据中的其他集合不受影响。
    pub fn drop_collection(&self, db: &str, coll: &str) -> MdbResult<bool> {
        let ns = Self::meta_key(db, coll);
        let removed = self.metas.lock().map_err(lock_err)?.remove(&ns);
        if removed.is_none() {
            return Ok(false);
        }
        self.delete_meta(&ns)?;
        let _ = self.engine.drop_cf(&Self::cf_name(db, coll));
        Ok(true)
    }

    /// 为集合分配一个新的记录号。
    ///
    /// 计数器持久化在元数据里，因此重启后不会与历史记录号冲突。
    pub fn next_record_id(&self, db: &str, coll: &str) -> MdbResult<i64> {
        let ns = Self::meta_key(db, coll);
        let mut metas = self.metas.lock().map_err(lock_err)?;
        let meta = metas
            .entry(ns.clone())
            .or_insert_with(|| StoredCollectionMeta { next_record_id: 1, ..Default::default() });
        let id = meta.next_record_id;
        meta.next_record_id += 1;
        let snapshot = meta.clone();
        drop(metas);
        self.write_meta(&ns, &snapshot)?;
        Ok(id)
    }

    /// 读取集合的全部键值对（已按键排序）。
    pub fn scan_all(&self, db: &str, coll: &str) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let cf = Self::cf_name(db, coll);
        self.engine.scan_all(&cf, b"")
    }

    /// 写入一条记录。
    pub fn put(&self, db: &str, coll: &str, key: Vec<u8>, value: Vec<u8>) -> MdbResult<()> {
        let cf = Self::cf_name(db, coll);
        self.engine.put(&cf, key, value)
    }

    /// 按精确键读取。
    pub fn get(&self, db: &str, coll: &str, key: &[u8]) -> MdbResult<Option<Vec<u8>>> {
        let cf = Self::cf_name(db, coll);
        self.engine.get(&cf, key)
    }

    /// 删除一条记录。
    pub fn delete(&self, db: &str, coll: &str, key: &[u8]) -> MdbResult<()> {
        let cf = Self::cf_name(db, coll);
        self.engine.delete(&cf, key)
    }

    /// 读取集合的索引定义。
    pub fn indexes(&self, db: &str, coll: &str) -> Vec<LiveIndexSpec> {
        self.metas
            .lock()
            .map(|m| {
                m.get(&Self::meta_key(db, coll))
                    .map(|meta| {
                        meta.indexes
                            .iter()
                            .map(|ix| LiveIndexSpec {
                                name: ix.name.clone(),
                                fields: ix.fields.clone(),
                                unique: ix.unique,
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }

    /// 覆盖写入集合的索引定义列表。
    pub fn set_indexes(&self, db: &str, coll: &str, specs: Vec<StoredIndexSpec>) -> MdbResult<()> {
        let ns = Self::meta_key(db, coll);
        let mut metas = self.metas.lock().map_err(lock_err)?;
        let meta = metas.entry(ns.clone()).or_default();
        meta.indexes = specs;
        let snapshot = meta.clone();
        drop(metas);
        self.write_meta(&ns, &snapshot)
    }

    /// 更新指定索引的 TTL 过期时间。
    pub fn update_index_expire(&self, db: &str, coll: &str, index_name: &str, expire_after_seconds: Option<i64>) -> MdbResult<bool> {
        let ns = Self::meta_key(db, coll);
        let mut metas = self.metas.lock().map_err(lock_err)?;
        let meta = match metas.get_mut(&ns) {
            Some(m) => m,
            None => return Ok(false),
        };
        let mut updated = false;
        for ix in meta.indexes.iter_mut() {
            if ix.name == index_name {
                ix.expire_after_seconds = expire_after_seconds;
                updated = true;
            }
        }
        if updated {
            let snapshot = meta.clone();
            drop(metas);
            self.write_meta(&ns, &snapshot)?;
        }
        Ok(updated)
    }

    // ------------------------------------------------------------------
    // 分片元数据持久化
    // ------------------------------------------------------------------

    /// 保存分片配置到元数据列族。
    pub fn save_shard_config(&self, config: &mdb_router::ConfigDb) -> MdbResult<()> {
        let key = b"sharding_config".to_vec();
        let config_doc = {
            let mut d = Document::new();
            // 保存分片
            let shards: Vec<Bson> = config.get_all_shards().iter().map(|s| {
                Bson::Document({
                    let mut sd = Document::new();
                    sd.insert("_id", Bson::String(s.id.to_string()));
                    sd.insert("host", Bson::String(s.host.clone()));
                    sd.insert("state", Bson::String(s.state.as_str().to_string()));
                    sd
                })
            }).collect();
            d.insert("shards", Bson::Array(shards));
            // 保存数据库
            let databases: Vec<Bson> = config.get_all_databases().iter().map(|(db, shard)| {
                Bson::Document({
                    let mut dd = Document::new();
                    dd.insert("db", Bson::String((*db).clone()));
                    dd.insert("primary", Bson::String(shard.to_string()));
                    dd
                })
            }).collect();
            d.insert("databases", Bson::Array(databases));
            // 保存分片键
            let shard_keys: Vec<Bson> = config.get_all_shard_keys().iter().map(|(ns, key)| {
                Bson::Document({
                    let mut kd = Document::new();
                    kd.insert("ns", Bson::String((*ns).clone()));
                    let key_doc = match key {
                        mdb_router::ShardKey::Hashed { field } => { let mut k = Document::new(); k.insert(field, Bson::String("hashed".into())); Bson::Document(k) }
                        mdb_router::ShardKey::Range { field } => { let mut k = Document::new(); k.insert(field, Bson::Int32(1)); Bson::Document(k) }
                        mdb_router::ShardKey::Compound { fields } => { let mut k = Document::new(); for f in fields { k.insert(f.clone(), Bson::Int32(1)); } Bson::Document(k) }
                    };
                    kd.insert("key", key_doc);
                    kd
                })
            }).collect();
            d.insert("shardKeys", Bson::Array(shard_keys));
            d
        };
        let value = mdb_doc::codec::to_bytes(&config_doc).map_err(|e| MdbError::new(ErrorCode::BadValue, format!("encode config: {e}")))?;
        self.engine.put(META_CF, key, value)
    }

    /// 从元数据列族加载分片配置。
    pub fn load_shard_config(&self) -> MdbResult<Option<mdb_router::ConfigDb>> {
        let key = b"sharding_config".to_vec();
        match self.engine.get(META_CF, &key) {
            Ok(Some(value)) => {
                let doc = mdb_doc::codec::from_bytes(&value).map_err(|e| MdbError::new(ErrorCode::BadValue, format!("decode config: {e}")))?;
                let mut config = mdb_router::ConfigDb::new();
                // 加载分片
                if let Some(shards) = doc.get("shards").and_then(|v| v.as_array()) {
                    for s in shards {
                        if let Some(sd) = s.as_document() {
                            let id = sd.get_str("_id").unwrap_or("").to_string();
                            let host = sd.get_str("host").unwrap_or("").to_string();
                            config.add_shard(mdb_router::ShardId::new(&id), host);
                        }
                    }
                }
                // 加载数据库
                if let Some(dbs) = doc.get("databases").and_then(|v| v.as_array()) {
                    for d in dbs {
                        if let Some(dd) = d.as_document() {
                            let db = dd.get_str("db").unwrap_or("").to_string();
                            let primary = dd.get_str("primary").unwrap_or("").to_string();
                            config.add_database(&db, mdb_router::ShardId::new(&primary));
                        }
                    }
                }
                // 加载分片键
                if let Some(keys) = doc.get("shardKeys").and_then(|v| v.as_array()) {
                    for k in keys {
                        if let Some(kd) = k.as_document() {
                            let ns = kd.get_str("ns").unwrap_or("").to_string();
                            if let Some(key_doc) = kd.get("key").and_then(|v| v.as_document()) {
                                // 解析分片键
                                let mut fields = Vec::new();
                                let mut is_hashed = false;
                                for (f, v) in key_doc.iter() {
                                    if let Some(s) = v.as_str() {
                                        if s == "hashed" { is_hashed = true; }
                                    }
                                    fields.push(f.to_string());
                                }
                                let key = if is_hashed {
                                    mdb_router::ShardKey::Hashed { field: fields[0].clone() }
                                } else if fields.len() == 1 {
                                    mdb_router::ShardKey::Range { field: fields[0].clone() }
                                } else {
                                    mdb_router::ShardKey::Compound { fields }
                                };
                                config.set_shard_key(&ns, key);
                            }
                        }
                    }
                }
                Ok(Some(config))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// 复制集合数据到另一个命名空间（用于分片迁移）。
    pub fn copy_collection_data(&self, from_db: &str, from_coll: &str, to_db: &str, to_coll: &str) -> MdbResult<usize> {
        // 读取源集合数据
        let from_cf = Self::cf_name(from_db, from_coll);
        let all_data = self.engine.scan_all(&from_cf, b"")?;
        
        // 写入目标集合
        let to_cf = Self::cf_name(to_db, to_coll);
        let mut copied = 0usize;
        for (key, value) in all_data {
            self.engine.put(&to_cf, key, value)?;
            copied += 1;
        }
        Ok(copied)
    }

    // ------------------------------------------------------------------
    // 用户与数据库目录
    // ------------------------------------------------------------------

    /// 持久化用户记录。
    ///
    /// `credential` 已是 SCRAM 派生结果，按字节原样保存，不在元数据里存明文口令。
    pub fn put_user(
        &self,
        username: &str,
        database: &str,
        credential: &[u8],
        mechanism: &str,
        iterations: u32,
        roles: &[(String, String)],
    ) -> MdbResult<()> {
        let mut doc = Document::new();
        doc.insert("_id", Bson::String(format!("{username}@{database}")));
        doc.insert("user", Bson::String(username.to_owned()));
        doc.insert("db", Bson::String(database.to_owned()));
        doc.insert(
            "credential",
            Bson::Binary(mdb_doc::binary::Binary::generic(credential.to_vec())),
        );
        doc.insert("mechanism", Bson::String(mechanism.to_owned()));
        doc.insert("iterations", Bson::Int64(iterations as i64));
        let role_docs: Vec<Bson> = roles
            .iter()
            .map(|(r, d)| {
                let mut rd = Document::new();
                rd.insert("role", Bson::String(r.clone()));
                rd.insert("db", Bson::String(d.clone()));
                Bson::Document(rd)
            })
            .collect();
        doc.insert("roles", Bson::Array(role_docs));
        let key = format!("user.{username}@{database}").into_bytes();
        self.engine
            .put(META_CF, key, mdb_doc::codec::to_bytes(&doc)?)
    }

    /// 删除用户记录。
    pub fn delete_user(&self, username: &str, database: &str) -> MdbResult<()> {
        let key = format!("user.{username}@{database}").into_bytes();
        self.engine.delete(META_CF, &key)
    }

    /// 持久化一条完整的用户记录（含 SCRAM 全部派生材料）。
    ///
    /// `credential` 为 `StoredKey` 的编码：mechanism、iterations、salt、
    /// stored_key、server_key 逐字段保存，因此重启后能重建
    /// `StoredKey` 完成认证。元数据里不含明文口令。
    pub fn put_user_record(&self, record: &Document) -> MdbResult<()> {
        let user = record
            .get("user")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_err("user record missing 'user'"))?;
        let db = record
            .get("db")
            .and_then(|v| v.as_str())
            .ok_or_else(|| invalid_err("user record missing 'db'"))?;
        let key = format!("user.{user}@{db}").into_bytes();
        self.engine
            .put(META_CF, key, mdb_doc::codec::to_bytes(record)?)
    }

    /// 从持久化文档还原用户凭据。
    ///
    /// 缺字段或凭据不完整时返回 `None`，让调用方跳过而不是用残缺凭据认证。
    pub fn user_from_doc(doc: &Document) -> Option<ParsedUser> {
        let username = doc.get("user")?.as_str()?.to_owned();
        let database = doc.get("db")?.as_str()?.to_owned();
        let cred = doc.get("credential")?.as_document()?;
        let mechanism = cred.get("mechanism")?.as_str()?.to_owned();
        let iteration_count = cred.get("iterations")?.as_i64()? as u32;
        let salt = binary_field(cred.get("salt")?)?;
        let stored_key = binary_field(cred.get("storedKey")?)?;
        let server_key = binary_field(cred.get("serverKey")?)?;
        let roles = doc
            .get("roles")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|r| {
                        let rd = r.as_document()?;
                        Some((
                            rd.get("role")?.as_str()?.to_owned(),
                            rd.get("db").and_then(|d| d.as_str()).unwrap_or("").to_owned(),
                        ))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Some(ParsedUser { username, database, mechanism, iteration_count, salt, stored_key, server_key, roles })
    }

    /// 枚举全部用户记录。
    pub fn list_users(&self) -> Vec<Document> {
        let mut out = Vec::new();
        if let Ok(entries) = self
            .engine
            .range_scan(META_CF, b"user.", &prefix_end(b"user."))
        {
            for (_, value) in entries {
                if let Ok(doc) = mdb_doc::codec::from_bytes(&value) {
                    out.push(doc);
                }
            }
        }
        out
    }

    /// 持久化数据库目录。
    pub fn put_database(&self, name: &str) -> MdbResult<()> {
        let mut doc = Document::new();
        doc.insert("_id", Bson::String(name.to_owned()));
        self.engine.put(
            META_CF,
            format!("db.{name}").into_bytes(),
            mdb_doc::codec::to_bytes(&doc)?,
        )
    }

    /// 枚举数据库目录。
    pub fn list_databases_persisted(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Ok(entries) = self
            .engine
            .range_scan(META_CF, b"db.", &prefix_end(b"db."))
        {
            for (_, value) in entries {
                if let Ok(doc) = mdb_doc::codec::from_bytes(&value) {
                    if let Some(name) = doc.get_str("_id") {
                        out.push(name.to_owned());
                    }
                }
            }
        }
        out
    }

    // ------------------------------------------------------------------
    // 内部：元数据读写
    // ------------------------------------------------------------------

    fn write_meta(&self, ns: &str, meta: &StoredCollectionMeta) -> MdbResult<()> {
        let mut doc = Document::new();
        doc.insert("_id", Bson::String(ns.to_owned()));
        doc.insert("ns", Bson::String(ns.to_owned()));
        doc.insert("nextRecordId", Bson::Int64(meta.next_record_id));
        let ix: Vec<Bson> = meta
            .indexes
            .iter()
            .map(|spec| {
                let mut d = Document::new();
                d.insert("name", Bson::String(spec.name.clone()));
                d.insert(
                    "fields",
                    Bson::Array(spec.fields.iter().map(|f| Bson::String(f.clone())).collect()),
                );
                d.insert("unique", Bson::Boolean(spec.unique));
                d.insert("sparse", Bson::Boolean(spec.sparse));
                doc_insert_expire(&mut d, spec);
                doc_insert_partial(&mut d, spec);
                Bson::Document(d)
            })
            .collect();
        doc.insert("indexes", Bson::Array(ix));
        self.engine.put(
            META_CF,
            format!("collmeta.{ns}").into_bytes(),
            mdb_doc::codec::to_bytes(&doc)?,
        )
    }

    fn delete_meta(&self, ns: &str) -> MdbResult<()> {
        self.engine
            .delete(META_CF, &format!("collmeta.{ns}").as_bytes())
    }
}

fn doc_insert_expire(d: &mut Document, spec: &StoredIndexSpec) {
    let v = match spec.expire_after_seconds {
        Some(v) => Bson::Int64(v),
        None => Bson::Null,
    };
    d.insert("expireAfterSeconds", v);
}

fn doc_insert_partial(d: &mut Document, spec: &StoredIndexSpec) {
    let v = match &spec.partial_filter {
        Some(p) => Bson::Document(p.clone()),
        None => Bson::Null,
    };
    d.insert("partialFilterExpression", v);
}

fn decode_meta(doc: &Document) -> StoredCollectionMeta {
    let next_record_id = doc
        .get("nextRecordId")
        .and_then(|v| v.as_i64())
        .unwrap_or(1);
    let indexes = doc
        .get("indexes")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|b| b.as_document())
                .map(|d| StoredIndexSpec {
                    name: d.get_str("name").unwrap_or("").to_owned(),
                    fields: d
                        .get("fields")
                        .and_then(|v| v.as_array())
                        .map(|f| {
                            f.iter()
                                .filter_map(|x| x.as_str().map(str::to_owned))
                                .collect()
                        })
                        .unwrap_or_default(),
                    unique: matches!(d.get("unique"), Some(Bson::Boolean(true))),
                    sparse: matches!(d.get("sparse"), Some(Bson::Boolean(true))),
                    expire_after_seconds: d.get("expireAfterSeconds").and_then(|v| v.as_i64()),
                    partial_filter: d
                        .get("partialFilterExpression")
                        .and_then(|v| v.as_document())
                        .cloned(),
                })
                .collect()
        })
        .unwrap_or_default();
    StoredCollectionMeta {
        next_record_id,
        indexes,
    }
}

/// 计算前缀扫描的排他上界。
///
/// `range_scan` 的区间是 `[start, end)`。要取出所有以 `p` 开头的键，
/// 上界必须取"p 加一个比 p 大的最小后缀"。逐字节把最后一个可递增字节加一即可：
/// `b"user."` → `b"user/"`（`'.'`=0x2E → `'/'`=0x2F）。
///
/// 若全为 0xFF 则不存在更大的键，用 `0xFF..` 形式表示。
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 0xFF {
            end.push(last + 1);
            return end;
        }
    }
    // 前缀全为 0xFF：任何以它为前缀的键都不小于前缀，用 0xFF 自身作排他上界会漏数据。
    // 追加一字节使上界严格大于所有前缀匹配键。
    vec![0xFF; prefix.len() + 1]
}

fn lock_err<T>(e: std::sync::PoisonError<T>) -> MdbError {
    MdbError::new(
        ErrorCode::DataCorruption,
        format!("catalog lock poisoned: {e}"),
    )
}

/// 调用方传入的元数据/参数不合法。
fn invalid_err(msg: &str) -> MdbError {
    MdbError::new(ErrorCode::IllegalOperation, msg.to_owned())
}

/// 列族名净化：仅保留 ASCII 字母数字、`_`、`-`，其余替换为 `_`。
///
/// 列族名会拼进 `colcf/cf_{id}` 与 manifest，必须防止路径穿越（`../`）。
fn sanitize_cf_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

// ===========================================================================
// CollectionStore
// ===========================================================================

/// 集合文档存储后端。
///
/// * `Memory`：纯内存，仅用于单元测试与 `--nojournal` 诊断模式。
/// * `Persistent`：接 LSM 存储引擎，写入先落 WAL，进程退出后数据仍在。
#[derive(Debug)]
pub enum StoreBackend {
    Memory(MemStoreData),
    Persistent {
        catalog: Arc<Catalog>,
        db: String,
        coll: String,
    },
}

/// 内存后端的裸数据。
#[derive(Debug, Clone, Default)]
pub struct MemStoreData {
    pub data: std::sync::Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl MemStoreData {
    pub fn new() -> Self {
        Self { data: std::sync::Arc::new(Mutex::new(BTreeMap::new())) }
    }
}

/// 与旧 `MemStore` 同名的门面，屏蔽后端差异。
#[derive(Debug)]
pub struct CollectionStore {
    backend: StoreBackend,
}

impl CollectionStore {
    pub fn in_memory() -> Self {
        Self { backend: StoreBackend::Memory(MemStoreData::new()) }
    }

    pub fn persistent(catalog: Arc<Catalog>, db: &str, coll: &str) -> Self {
        Self {
            backend: StoreBackend::Persistent {
                catalog,
                db: db.to_owned(),
                coll: coll.to_owned(),
            },
        }
    }

    pub fn is_persistent(&self) -> bool {
        matches!(self.backend, StoreBackend::Persistent { .. })
    }

    pub fn get(&self, key: &[u8]) -> MdbResult<Option<Vec<u8>>> {
        match &self.backend {
            StoreBackend::Memory(m) => {
                let d = m.data.lock().map_err(lock_err)?;
                Ok(d.get(key).cloned())
            }
            StoreBackend::Persistent { catalog, db, coll } => catalog.get(db, coll, key),
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> MdbResult<()> {
        match &self.backend {
            StoreBackend::Memory(m) => {
                let mut d = m.data.lock().map_err(lock_err)?;
                d.insert(key, value);
                Ok(())
            }
            StoreBackend::Persistent { catalog, db, coll } => catalog.put(db, coll, key, value),
        }
    }

    pub fn delete(&self, key: &[u8]) -> MdbResult<bool> {
        match &self.backend {
            StoreBackend::Memory(m) => {
                let mut d = m.data.lock().map_err(lock_err)?;
                Ok(d.remove(key).is_some())
            }
            StoreBackend::Persistent { catalog, db, coll } => {
                let existed = catalog.get(db, coll, key)?.is_some();
                catalog.delete(db, coll, key)?;
                Ok(existed)
            }
        }
    }

    /// 读取全部文档。
    ///
    /// `prefix` 为空表示全集合扫描；非空时只返回该前缀下的键。
    pub fn scan(&self, prefix: &[u8]) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        match &self.backend {
            StoreBackend::Memory(m) => {
                let d = m.data.lock().map_err(lock_err)?;
                Ok(if prefix.is_empty() {
                    d.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                } else {
                    let start = prefix.to_vec();
                    d.range(start..)
                        .take_while(|(k, _)| k.starts_with(prefix))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                })
            }
            StoreBackend::Persistent { catalog, db, coll } => {
                let all = catalog.scan_all(db, coll)?;
                if prefix.is_empty() {
                    Ok(all)
                } else {
                    Ok(all
                        .into_iter()
                        .filter(|(k, _)| k.starts_with(prefix))
                        .collect())
                }
            }
        }
    }

    /// 全集合扫描。`scan(b"")` 的显式别名，便于阅读。
    pub fn scan_all(&self) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan(b"")
    }

    pub fn len(&self) -> MdbResult<usize> {
        match &self.backend {
            StoreBackend::Memory(m) => {
                let d = m.data.lock().map_err(lock_err)?;
                Ok(d.len())
            }
            StoreBackend::Persistent { catalog, db, coll } => Ok(catalog.scan_all(db, coll)?.len()),
        }
    }

    pub fn is_empty(&self) -> MdbResult<bool> {
        Ok(self.len()? == 0)
    }
}

// ===========================================================================
// 测试
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("mdb_catalog_{}_{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn doc_id(id: i32) -> Document {
        let mut d = Document::new();
        d.insert("_id", Bson::Int32(id));
        d.insert("name", Bson::String(format!("user{id}")));
        d
    }

    #[test]
    fn put_get_roundtrip() {
        let dir = temp_dir();
        let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
        cat.create_collection("db", "c").unwrap();
        cat.put(
            "db",
            "c",
            b"k1".to_vec(),
            mdb_doc::codec::to_bytes(&doc_id(1)).unwrap(),
        )
        .unwrap();

        let got = cat.get("db", "c", b"k1").unwrap().unwrap();
        let back = mdb_doc::codec::from_bytes(&got).unwrap();
        assert_eq!(back.get_i64("_id"), Some(1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn data_survives_reopen() {
        let dir = temp_dir();
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            cat.create_collection("db", "c").unwrap();
            for i in 0..50 {
                cat.put(
                    "db",
                    "c",
                    format!("k{i}").into_bytes(),
                    mdb_doc::codec::to_bytes(&doc_id(i)).unwrap(),
                )
                .unwrap();
            }
        }
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            assert!(cat.collection_exists("db", "c"));
            let all = cat.scan_all("db", "c").unwrap();
            assert_eq!(all.len(), 50);
            assert!(cat.get("db", "c", b"k7").unwrap().is_some());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn create_collection_is_idempotent() {
        let dir = temp_dir();
        let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
        assert!(cat.create_collection("db", "c").unwrap());
        assert!(!cat.create_collection("db", "c").unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn drop_collection_removes_data() {
        let dir = temp_dir();
        let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
        cat.create_collection("db", "c").unwrap();
        cat.put("db", "c", b"k".to_vec(), b"v".to_vec()).unwrap();
        assert!(cat.drop_collection("db", "c").unwrap());
        assert!(!cat.collection_exists("db", "c"));
        assert!(!cat.drop_collection("db", "c").unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn record_id_monotonic_across_reopen() {
        let dir = temp_dir();
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            cat.create_collection("db", "c").unwrap();
            let a = cat.next_record_id("db", "c").unwrap();
            let b = cat.next_record_id("db", "c").unwrap();
            assert!(b > a);
        }
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            let c = cat.next_record_id("db", "c").unwrap();
            // 重启后必须继续递增，不能复用重启前的编号
            assert!(c > 2, "record id regressed after reopen: {c}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn indexes_persist() {
        let dir = temp_dir();
        let specs = vec![StoredIndexSpec {
            name: "name_1".into(),
            fields: vec!["name".into()],
            unique: true,
            sparse: false,
            expire_after_seconds: Some(60),
            partial_filter: None,
        }];
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            cat.create_collection("db", "c").unwrap();
            cat.set_indexes("db", "c", specs).unwrap();
        }
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            let ix = cat.indexes("db", "c");
            assert_eq!(ix.len(), 1);
            assert_eq!(ix[0].name, "name_1");
            assert!(ix[0].unique);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn users_persist() {
        let dir = temp_dir();
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            cat.put_user(
                "alice",
                "admin",
                b"cred-bytes",
                "SCRAM-SHA-256",
                15000,
                &[("root".into(), "admin".into())],
            )
            .unwrap();
        }
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            let users = cat.list_users();
            assert_eq!(users.len(), 1);
            assert_eq!(users[0].get_str("user"), Some("alice"));
            let roles = users[0].get("roles").and_then(|v| v.as_array()).unwrap();
            assert_eq!(roles.len(), 1);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_user_persists() {
        let dir = temp_dir();
        let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
        cat.put_user("bob", "admin", b"c", "SCRAM-SHA-256", 15000, &[])
            .unwrap();
        assert_eq!(cat.list_users().len(), 1);
        cat.delete_user("bob", "admin").unwrap();
        assert_eq!(cat.list_users().len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn databases_persist() {
        let dir = temp_dir();
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            cat.put_database("mydb").unwrap();
            cat.put_database("other").unwrap();
        }
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            let mut dbs = cat.list_databases_persisted();
            dbs.sort();
            assert_eq!(dbs, vec!["mydb".to_string(), "other".to_string()]);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn namespaces_listed_after_reopen() {
        let dir = temp_dir();
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            cat.create_collection("a", "x").unwrap();
            cat.create_collection("b", "y").unwrap();
        }
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            let mut ns = cat.list_namespaces();
            ns.sort();
            assert_eq!(ns, vec!["a.x".to_string(), "b.y".to_string()]);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_record() {
        let dir = temp_dir();
        let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
        cat.create_collection("db", "c").unwrap();
        cat.put("db", "c", b"k".to_vec(), b"v".to_vec()).unwrap();
        cat.delete("db", "c", b"k").unwrap();
        assert!(cat.get("db", "c", b"k").unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cf_name_rejects_path_traversal() {
        // db 名 "../../etc" 不得逃出 dbpath
        let n = Catalog::cf_name("../../etc", "passwd");
        assert!(!n.contains(".."), "cf name must not contain '..': {n}");
        assert!(!n.contains('/'), "cf name must not contain '/': {n}");
    }

    #[test]
    fn two_collections_are_isolated() {
        let dir = temp_dir();
        let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
        cat.create_collection("db", "a").unwrap();
        cat.create_collection("db", "b").unwrap();
        cat.put("db", "a", b"same".to_vec(), b"A".to_vec()).unwrap();
        cat.put("db", "b", b"same".to_vec(), b"B".to_vec()).unwrap();
        assert_eq!(cat.get("db", "a", b"same").unwrap().unwrap(), b"A".to_vec());
        assert_eq!(cat.get("db", "b", b"same").unwrap().unwrap(), b"B".to_vec());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn many_records_survive_reopen() {
        let dir = temp_dir();
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            cat.create_collection("db", "big").unwrap();
            for i in 0..500i32 {
                cat.put(
                    "db",
                    "big",
                    format!("k{i:05}").into_bytes(),
                    mdb_doc::codec::to_bytes(&doc_id(i)).unwrap(),
                )
                .unwrap();
            }
        }
        {
            let cat = Catalog::open(&dir, StorageConfig::default()).unwrap();
            let all = cat.scan_all("db", "big").unwrap();
            assert_eq!(all.len(), 500);
            for (i, (_, v)) in all.iter().enumerate() {
                let d = mdb_doc::codec::from_bytes(v).unwrap();
                assert_eq!(d.get_i64("_id"), Some(i as i64));
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}