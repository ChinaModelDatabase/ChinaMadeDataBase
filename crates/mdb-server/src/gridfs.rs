//! GridFS：大文件分块存储（兼容 MongoDB GridFS 规范）。
//!
//! # 设计
//!
//! - 文件元数据存入 `{prefix}.files`，数据块存入 `{prefix}.chunks`。
//! - 默认 chunk_size = 255 KiB（与 MongoDB 官方一致）。
//! - 通过 [`GridFsStorage`] trait 抽象存储层，避免直接依赖 `mdb-storage`。
//! - 所有公开类型均有 `#[derive(Debug)]`，满足 `mdb-server` 的 lint 要求。

#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use mdb_common::{ErrorCode, MdbError, MdbResult};
use mdb_doc::{Bson, Document, ObjectId};

// ---------------------------------------------------------------------------
// 错误码
// ---------------------------------------------------------------------------

/// GridFS 专属错误码（复用 MongoDB 现有码）。
fn err_file_not_found(id: &ObjectId) -> MdbError {
    MdbError::new(
        ErrorCode::IllegalOperation,
        format!("GridFS file not found: {id}"),
    )
}

fn err_chunk_not_found(file_id: &ObjectId, n: i32) -> MdbError {
    MdbError::new(
        ErrorCode::IllegalOperation,
        format!("GridFS chunk not found: file_id={file_id}, n={n}"),
    )
}

fn err_invalid_chunk_size(size: i32) -> MdbError {
    MdbError::new(
        ErrorCode::BadValue,
        format!("invalid chunk size: {size}"),
    )
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// 默认分块大小：255 KiB（与 MongoDB 一致）。
pub const DEFAULT_CHUNK_SIZE: i32 = 255 * 1024;

/// GridFS 文件元数据（对应 `{prefix}.files` 集合中的文档）。
#[derive(Debug, Clone, PartialEq)]
pub struct GridFsFile {
    /// 文件唯一标识。
    pub _id: ObjectId,
    /// 文件总长度（字节）。
    pub length: i64,
    /// 每个数据块的最大字节数。
    pub chunk_size: i32,
    /// 文件名（可为空）。
    pub filename: String,
    /// MIME 类型（默认 `application/octet-stream`）。
    pub content_type: String,
    /// 用户自定义元数据。
    pub metadata: Document,
    /// 上传时间（Unix 毫秒）。
    pub upload_date: i64,
}

impl GridFsFile {
    /// 从 BSON Document 反序列化。
    pub fn from_document(doc: &Document) -> Option<Self> {
        let id = match doc.get("_id")? {
            Bson::ObjectId(oid) => *oid,
            _ => return None,
        };
        let length = match doc.get("length")? {
            Bson::Int64(v) => *v,
            Bson::Int32(v) => *v as i64,
            Bson::Double(v) => *v as i64,
            _ => return None,
        };
        let chunk_size = match doc.get("chunkSize")? {
            Bson::Int32(v) => *v,
            Bson::Int64(v) => *v as i32,
            Bson::Double(v) => *v as i32,
            _ => return None,
        };
        let filename = match doc.get("filename")? {
            Bson::String(s) => s.clone(),
            _ => return None,
        };
        let content_type = match doc.get("contentType") {
            Some(Bson::String(s)) => s.clone(),
            _ => "application/octet-stream".to_owned(),
        };
        let metadata = match doc.get("metadata") {
            Some(Bson::Document(d)) => d.clone(),
            _ => Document::new(),
        };
        let upload_date = match doc.get("uploadDate")? {
            Bson::DateTime(v) => *v,
            Bson::Int64(v) => *v,
            Bson::Int32(v) => *v as i64,
            _ => return None,
        };
        Some(Self {
            _id: id,
            length,
            chunk_size,
            filename,
            content_type,
            metadata,
            upload_date,
        })
    }

    /// 序列化为 BSON Document。
    pub fn to_document(&self) -> Document {
        let mut doc = Document::new();
        doc.insert("_id", self._id);
        doc.insert("length", self.length);
        doc.insert("chunkSize", self.chunk_size);
        doc.insert("filename", self.filename.clone());
        doc.insert("contentType", self.content_type.clone());
        if !self.metadata.is_empty() {
            doc.insert("metadata", Bson::Document(self.metadata.clone()));
        }
        doc.insert("uploadDate", self.upload_date);
        doc
    }
}

/// GridFS 数据块（对应 `{prefix}.chunks` 集合中的文档）。
#[derive(Debug, Clone, PartialEq)]
pub struct GridFsChunk {
    /// 块唯一标识。
    pub _id: ObjectId,
    /// 所属文件的 `_id`。
    pub files_id: ObjectId,
    /// 块序号（从 0 开始）。
    pub n: i32,
    /// 块数据。
    pub data: Vec<u8>,
}

impl GridFsChunk {
    /// 从 BSON Document 反序列化。
    pub fn from_document(doc: &Document) -> Option<Self> {
        let _id = match doc.get("_id")? {
            Bson::ObjectId(oid) => *oid,
            _ => return None,
        };
        let files_id = match doc.get("files_id")? {
            Bson::ObjectId(oid) => *oid,
            _ => return None,
        };
        let n = match doc.get("n")? {
            Bson::Int32(v) => *v,
            Bson::Int64(v) => *v as i32,
            _ => return None,
        };
        let data = match doc.get("data")? {
            Bson::Binary(bin) => bin.bytes.clone(),
            _ => return None,
        };
        Some(Self {
            _id,
            files_id,
            n,
            data,
        })
    }

    /// 序列化为 BSON Document。
    pub fn to_document(&self) -> Document {
        use mdb_doc::Binary;
        let mut doc = Document::new();
        doc.insert("_id", self._id);
        doc.insert("files_id", self.files_id);
        doc.insert("n", self.n);
        doc.insert("data", Bson::Binary(Binary::generic(self.data.clone())));
        doc
    }
}

// ---------------------------------------------------------------------------
// Storage trait
// ---------------------------------------------------------------------------

/// 存储层抽象：GridFS 通过此 trait 与后端交互。
///
/// 实现者需要保证：
/// - `insert_one` 生成的 ObjectId 不能与已有文档冲突。
/// - `delete_one` 在文档不存在时返回 `Ok(false)`。
/// - `find_many` 返回的所有文档需包含完整字段。
pub trait GridFsStorage: Send + Sync {
    /// 插入一条文档，返回生成的 `_id`。
    fn insert_one(&self, ns: &str, doc: Document) -> MdbResult<ObjectId>;

    /// 按过滤条件查找一条文档。
    fn find_one(&self, ns: &str, filter: &Document) -> MdbResult<Option<Document>>;

    /// 按过滤条件查找多条文档。
    fn find_many(&self, ns: &str, filter: &Document) -> MdbResult<Vec<Document>>;

    /// 删除一条文档。
    fn delete_one(&self, ns: &str, filter: &Document) -> MdbResult<bool>;

    /// 删除多条文档。
    fn delete_many(&self, ns: &str, filter: &Document) -> MdbResult<u64>;

    /// 更新一条文档。
    fn update_one(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
    ) -> MdbResult<bool>;
}

// ---------------------------------------------------------------------------
// Bucket
// ---------------------------------------------------------------------------

/// GridFS Bucket：提供文件的上传、下载、查找、删除、重命名操作。
pub struct GridFsBucket {
    db_name: String,
    prefix: String,
    chunk_size: i32,
    storage: Arc<dyn GridFsStorage>,
}

impl std::fmt::Debug for GridFsBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GridFsBucket")
            .field("db_name", &self.db_name)
            .field("prefix", &self.prefix)
            .field("chunk_size", &self.chunk_size)
            .field("storage", &"<dyn GridFsStorage>")
            .finish()
    }
}

impl GridFsBucket {
    /// 使用默认前缀 `"fs"` 构造。
    pub fn new(db_name: impl Into<String>, storage: Arc<dyn GridFsStorage>) -> Self {
        Self::with_prefix(db_name, "fs", storage)
    }

    /// 指定前缀和默认 chunk size 构造。
    pub fn with_prefix(
        db_name: impl Into<String>,
        prefix: impl Into<String>,
        storage: Arc<dyn GridFsStorage>,
    ) -> Self {
        Self {
            db_name: db_name.into(),
            prefix: prefix.into(),
            chunk_size: DEFAULT_CHUNK_SIZE,
            storage,
        }
    }

    /// 覆盖默认 chunk size。
    pub fn with_chunk_size(mut self, chunk_size: i32) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// `{prefix}.files` 集合的完整命名空间。
    fn files_ns(&self) -> String {
        format!("{}.{}", self.db_name, self.prefix)
    }

    /// `{prefix}.chunks` 集合的完整命名空间。
    fn chunks_ns(&self) -> String {
        format!("{}.{}.chunks", self.db_name, self.prefix)
    }

    /// 一次性上传完整数据，返回文件 `_id`。
    ///
    /// # 参数
    /// - `filename`: 文件名（用于后续查找）。
    /// - `data`: 文件内容。
    /// - `content_type`: MIME 类型（传 `None` 时使用默认值）。
    /// - `metadata`: 额外元数据（传 `None` 时使用空文档）。
    pub fn upload_from_bytes(
        &self,
        filename: &str,
        data: &[u8],
        content_type: Option<&str>,
        metadata: Option<Document>,
    ) -> MdbResult<ObjectId> {
        let ct = content_type.unwrap_or("application/octet-stream").to_owned();
        let meta = metadata.unwrap_or_default();
        let now = unix_millis();
        let file_id = ObjectId::new().ok_or_else(|| {
            MdbError::new(ErrorCode::IllegalOperation, "ObjectId counter exhausted")
        })?;

        let file_doc = GridFsFile {
            _id: file_id,
            length: data.len() as i64,
            chunk_size: self.chunk_size,
            filename: filename.to_owned(),
            content_type: ct,
            metadata: meta,
            upload_date: now,
        };

        // 先插入 chunks
        let chunks = chunk_data(data, self.chunk_size);
        for (n, chunk_bytes) in chunks.into_iter().enumerate() {
            let chunk = GridFsChunk {
                _id: ObjectId::new().ok_or_else(|| {
                    MdbError::new(ErrorCode::IllegalOperation, "ObjectId counter exhausted")
                })?,
                files_id: file_id,
                n: n as i32,
                data: chunk_bytes,
            };
            self.storage
                .insert_one(&self.chunks_ns(), chunk.to_document())?;
        }

        // 再插入 files 文档
        self.storage
            .insert_one(&self.files_ns(), file_doc.to_document())?;

        Ok(file_id)
    }

    /// 下载整个文件到内存。
    pub fn download_to_bytes(&self, file_id: ObjectId) -> MdbResult<Vec<u8>> {
        let file = self.find_file_by_id(file_id)?;

        let mut filter = Document::new();
        filter.insert("files_id", Bson::ObjectId(file_id));

        let mut chunks = self.storage.find_many(&self.chunks_ns(), &filter)?;
        // 按 n 排序
        chunks.sort_by_key(|doc| match doc.get("n") {
            Some(Bson::Int32(v)) => *v,
            Some(Bson::Int64(v)) => *v as i32,
            _ => 0,
        });

        let expected_chunks = if file.chunk_size > 0 {
            ((file.length as f64) / (file.chunk_size as f64)).ceil() as usize
        } else {
            0
        };

        if chunks.len() != expected_chunks {
            return Err(MdbError::new(
                ErrorCode::IllegalOperation,
                format!(
                    "GridFS file {file_id}: expected {expected_chunks} chunks, got {}",
                    chunks.len()
                ),
            ));
        }

        let total = file.length as usize;
        let mut buf = Vec::with_capacity(total);
        for chunk_doc in &chunks {
            match chunk_doc.get("data") {
                Some(Bson::Binary(bin)) => buf.extend_from_slice(&bin.bytes),
                _ => {
                    return Err(MdbError::new(
                        ErrorCode::IllegalOperation,
                        "GridFS chunk missing or invalid data field",
                    ));
                }
            }
        }

        // 对齐最后一个 chunk 的 padding
        if buf.len() > total {
            buf.truncate(total);
        }

        Ok(buf)
    }

    /// 按文件名查找所有匹配的文件元数据。
    pub fn find_by_filename(&self, filename: &str) -> MdbResult<Vec<GridFsFile>> {
        let mut filter = Document::new();
        filter.insert("filename", filename);
        let docs = self.storage.find_many(&self.files_ns(), &filter)?;
        Ok(docs
            .iter()
            .filter_map(GridFsFile::from_document)
            .collect())
    }

    /// 按任意过滤条件查找文件。
    pub fn find(&self, filter: &Document) -> MdbResult<Vec<GridFsFile>> {
        let docs = self.storage.find_many(&self.files_ns(), filter)?;
        Ok(docs
            .iter()
            .filter_map(GridFsFile::from_document)
            .collect())
    }

    /// 删除文件及其所有 chunk。
    pub fn delete(&self, file_id: ObjectId) -> MdbResult<bool> {
        let mut files_filter = Document::new();
        files_filter.insert("_id", Bson::ObjectId(file_id));

        let mut chunks_filter = Document::new();
        chunks_filter.insert("files_id", Bson::ObjectId(file_id));

        self.storage
            .delete_many(&self.chunks_ns(), &chunks_filter)?;
        let deleted = self
            .storage
            .delete_one(&self.files_ns(), &files_filter)?;
        Ok(deleted)
    }

    /// 重命名文件（更新 filename 字段）。
    pub fn rename(&self, file_id: ObjectId, new_filename: &str) -> MdbResult<bool> {
        let mut filter = Document::new();
        filter.insert("_id", Bson::ObjectId(file_id));

        let mut update = Document::new();
        // 使用 $set 操作符
        let mut set_doc = Document::new();
        set_doc.insert("filename", new_filename);
        update.insert("$set", Bson::Document(set_doc));

        self.storage
            .update_one(&self.files_ns(), &filter, &update)
    }

    /// 按 _id 查找文件元数据。
    fn find_file_by_id(&self, file_id: ObjectId) -> MdbResult<GridFsFile> {
        let mut filter = Document::new();
        filter.insert("_id", Bson::ObjectId(file_id));
        let doc = self
            .storage
            .find_one(&self.files_ns(), &filter)?
            .ok_or_else(|| err_file_not_found(&file_id))?;
        GridFsFile::from_document(&doc)
            .ok_or_else(|| MdbError::new(ErrorCode::FailedToParse, "invalid GridFS file document"))
    }
}

// ---------------------------------------------------------------------------
// Chunker
// ---------------------------------------------------------------------------

/// 将数据切分为固定大小的块。
///
/// 最后一个块可能小于 `chunk_size`。
///
/// # Panics
///
/// 如果 `chunk_size <= 0`，会 panic。
pub fn chunk_data(data: &[u8], chunk_size: i32) -> Vec<Vec<u8>> {
    assert!(chunk_size > 0, "chunk_size must be > 0, got {chunk_size}");
    let cs = chunk_size as usize;
    if data.is_empty() {
        return vec![];
    }
    data.chunks(cs).map(|c| c.to_vec()).collect()
}

/// 从块列表重建完整数据。
pub fn reconstruct_data(chunks: Vec<Vec<u8>>) -> Vec<u8> {
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    let mut buf = Vec::with_capacity(total);
    for chunk in chunks {
        buf.extend_from_slice(&chunk);
    }
    buf
}

/// 从二进制切片流式重建数据（按 n 顺序排列的块）。
pub fn reconstruct_from_binaries(chunks: &[(i32, &[u8])]) -> Vec<u8> {
    let total: usize = chunks.iter().map(|(_, d)| d.len()).sum();
    let mut buf = Vec::with_capacity(total);
    for (_, data) in chunks {
        buf.extend_from_slice(data);
    }
    buf
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

/// GridFS 命令处理器：解析 wire 层命令文档并分发到 Bucket 操作。
///
/// # 支持的命令
///
/// | 命令 | 描述 |
/// |------|------|
/// | `gridfs.upload` | 上传文件 |
/// | `gridfs.download` | 下载文件 |
/// | `gridfs.find` | 查找文件 |
/// | `gridfs.delete` | 删除文件 |
/// | `gridfs.rename` | 重命名文件 |
#[derive(Debug)]
pub struct GridFsCommandHandler {
    bucket: GridFsBucket,
}

impl GridFsCommandHandler {
    /// 创建命令处理器。
    pub fn new(bucket: GridFsBucket) -> Self {
        Self { bucket }
    }

    /// 处理 `gridfs.upload` 命令。
    ///
    /// 期望文档字段：
    /// - `filename`: String — 文件名
    /// - `data`: Binary — 文件内容
    /// - `contentType`: String（可选）— MIME 类型
    /// - `metadata`: Document（可选）— 额外元数据
    ///
    /// 返回：
    /// - `_id`: ObjectId — 上传文件的标识
    pub fn handle_upload(&self, cmd: &Document) -> MdbResult<Document> {
        let filename = match cmd.get("filename") {
            Some(Bson::String(s)) => s.as_str(),
            _ => {
                return Err(MdbError::new(
                    ErrorCode::FailedToParse,
                    "gridfs.upload: missing or invalid 'filename'",
                ));
            }
        };

        let data = match cmd.get("data") {
            Some(Bson::Binary(bin)) => bin.bytes.as_slice(),
            _ => {
                return Err(MdbError::new(
                    ErrorCode::FailedToParse,
                    "gridfs.upload: missing or invalid 'data'",
                ));
            }
        };

        let content_type = match cmd.get("contentType") {
            Some(Bson::String(s)) => Some(s.as_str()),
            _ => None,
        };

        let metadata = match cmd.get("metadata") {
            Some(Bson::Document(d)) => Some(d.clone()),
            _ => None,
        };

        let file_id = self
            .bucket
            .upload_from_bytes(filename, data, content_type, metadata)?;

        let mut reply = Document::new();
        reply.insert("_id", file_id);
        reply.insert("ok", 1.0f64);
        Ok(reply)
    }

    /// 处理 `gridfs.download` 命令。
    ///
    /// 期望文档字段：
    /// - `_id`: ObjectId — 文件标识
    ///
    /// 返回：
    /// - `data`: Binary — 文件内容
    /// - `length`: Int64 — 文件长度
    /// - `filename`: String — 文件名
    /// - `contentType`: String — MIME 类型
    pub fn handle_download(&self, cmd: &Document) -> MdbResult<Document> {
        let file_id = match cmd.get("_id") {
            Some(Bson::ObjectId(oid)) => *oid,
            _ => {
                return Err(MdbError::new(
                    ErrorCode::FailedToParse,
                    "gridfs.download: missing or invalid '_id'",
                ));
            }
        };

        let data = self.bucket.download_to_bytes(file_id)?;
        let file = self.bucket.find_file_by_id(file_id)?;

        let mut reply = Document::new();
        reply.insert("data", Bson::Binary(mdb_doc::Binary::generic(data)));
        reply.insert("length", file.length);
        reply.insert("filename", file.filename);
        reply.insert("contentType", file.content_type);
        reply.insert("ok", 1.0f64);
        Ok(reply)
    }

    /// 处理 `gridfs.find` 命令。
    ///
    /// 期望文档字段：
    /// - `filter`: Document（可选）— 查询条件
    ///
    /// 返回：
    /// - `files`: Array — 匹配的文件元数据文档列表
    pub fn handle_find(&self, cmd: &Document) -> MdbResult<Document> {
        let filter = match cmd.get("filter") {
            Some(Bson::Document(d)) => d.clone(),
            _ => Document::new(),
        };

        let files = self.bucket.find(&filter)?;

        let file_docs: Vec<Bson> = files.iter().map(|f| Bson::Document(f.to_document())).collect();

        let mut reply = Document::new();
        reply.insert("files", Bson::Array(file_docs));
        reply.insert("ok", 1.0f64);
        Ok(reply)
    }

    /// 处理 `gridfs.delete` 命令。
    ///
    /// 期望文档字段：
    /// - `_id`: ObjectId — 文件标识
    ///
    /// 返回：
    /// - `ok`: Double — 1.0 表示成功
    /// - `deleted`: Boolean — 是否成功删除
    pub fn handle_delete(&self, cmd: &Document) -> MdbResult<Document> {
        let file_id = match cmd.get("_id") {
            Some(Bson::ObjectId(oid)) => *oid,
            _ => {
                return Err(MdbError::new(
                    ErrorCode::FailedToParse,
                    "gridfs.delete: missing or invalid '_id'",
                ));
            }
        };

        let deleted = self.bucket.delete(file_id)?;

        let mut reply = Document::new();
        reply.insert("ok", 1.0f64);
        reply.insert("deleted", deleted);
        Ok(reply)
    }

    /// 处理 `gridfs.rename` 命令。
    ///
    /// 期望文档字段：
    /// - `_id`: ObjectId — 文件标识
    /// - `new_filename`: String — 新文件名
    pub fn handle_rename(&self, cmd: &Document) -> MdbResult<Document> {
        let file_id = match cmd.get("_id") {
            Some(Bson::ObjectId(oid)) => *oid,
            _ => {
                return Err(MdbError::new(
                    ErrorCode::FailedToParse,
                    "gridfs.rename: missing or invalid '_id'",
                ));
            }
        };

        let new_filename = match cmd.get("new_filename") {
            Some(Bson::String(s)) => s.as_str(),
            _ => {
                return Err(MdbError::new(
                    ErrorCode::FailedToParse,
                    "gridfs.rename: missing or invalid 'new_filename'",
                ));
            }
        };

        let renamed = self.bucket.rename(file_id, new_filename)?;

        let mut reply = Document::new();
        reply.insert("ok", 1.0f64);
        reply.insert("renamed", renamed);
        Ok(reply)
    }

    /// 根据命令名称分派到对应处理器。
    pub fn dispatch(&self, cmd: &Document) -> MdbResult<Document> {
        if cmd.contains_key("gridfs.upload") {
            // 从嵌套的命令文档中提取实际参数
            let inner = match cmd.get("gridfs.upload") {
                Some(Bson::Document(d)) => d,
                _ => {
                    return Err(MdbError::new(
                        ErrorCode::FailedToParse,
                        "gridfs.upload: command body must be a document",
                    ));
                }
            };
            return self.handle_upload(inner);
        }

        if cmd.contains_key("gridfs.download") {
            let inner = match cmd.get("gridfs.download") {
                Some(Bson::Document(d)) => d,
                _ => {
                    return Err(MdbError::new(
                        ErrorCode::FailedToParse,
                        "gridfs.download: command body must be a document",
                    ));
                }
            };
            return self.handle_download(inner);
        }

        if cmd.contains_key("gridfs.find") {
            let inner = match cmd.get("gridfs.find") {
                Some(Bson::Document(d)) => d,
                _ => {
                    return Err(MdbError::new(
                        ErrorCode::FailedToParse,
                        "gridfs.find: command body must be a document",
                    ));
                }
            };
            return self.handle_find(inner);
        }

        if cmd.contains_key("gridfs.delete") {
            let inner = match cmd.get("gridfs.delete") {
                Some(Bson::Document(d)) => d,
                _ => {
                    return Err(MdbError::new(
                        ErrorCode::FailedToParse,
                        "gridfs.delete: command body must be a document",
                    ));
                }
            };
            return self.handle_delete(inner);
        }

        if cmd.contains_key("gridfs.rename") {
            let inner = match cmd.get("gridfs.rename") {
                Some(Bson::Document(d)) => d,
                _ => {
                    return Err(MdbError::new(
                        ErrorCode::FailedToParse,
                        "gridfs.rename: command body must be a document",
                    ));
                }
            };
            return self.handle_rename(inner);
        }

        Err(MdbError::new(
            ErrorCode::CommandNotImplemented,
            format!(
                "unknown GridFS command: {:?}",
                cmd.keys().next().unwrap_or("<empty>")
            ),
        ))
    }
}

// ---------------------------------------------------------------------------
// In-memory storage (for testing)
// ---------------------------------------------------------------------------

/// 内存存储实现，用于测试和演示。
#[derive(Debug, Default)]
pub struct InMemoryStorage {
    collections: Mutex<HashMap<String, Vec<Document>>>,
}

impl InMemoryStorage {
    /// 创建新的空内存存储。
    pub fn new() -> Self {
        Self {
            collections: Mutex::new(HashMap::new()),
        }
    }

    fn get_or_create(&self, ns: &str) -> Vec<Document> {
        self.collections
            .lock()
            .expect("mutex poisoned")
            .entry(ns.to_owned())
            .or_default()
            .clone()
    }

    fn push_to(&self, ns: &str, doc: Document) {
        self.collections
            .lock()
            .expect("mutex poisoned")
            .entry(ns.to_owned())
            .or_default()
            .push(doc);
    }
}

impl GridFsStorage for InMemoryStorage {
    fn insert_one(&self, ns: &str, doc: Document) -> MdbResult<ObjectId> {
        let id = match doc.get("_id") {
            Some(Bson::ObjectId(oid)) => *oid,
            _ => ObjectId::new().ok_or_else(|| {
                MdbError::new(ErrorCode::IllegalOperation, "ObjectId counter exhausted")
            })?,
        };
        self.push_to(ns, doc);
        Ok(id)
    }

    fn find_one(&self, ns: &str, filter: &Document) -> MdbResult<Option<Document>> {
        let lock = self.collections.lock().expect("mutex poisoned");
        let docs = lock.get(ns).map(|v| v.as_slice()).unwrap_or(&[]);
        Ok(docs
            .iter()
            .find(|doc| document_matches(doc, filter))
            .cloned())
    }

    fn find_many(&self, ns: &str, filter: &Document) -> MdbResult<Vec<Document>> {
        let lock = self.collections.lock().expect("mutex poisoned");
        let docs = lock.get(ns).map(|v| v.as_slice()).unwrap_or(&[]);
        Ok(docs
            .iter()
            .filter(|doc| document_matches(doc, filter))
            .cloned()
            .collect())
    }

    fn delete_one(&self, ns: &str, filter: &Document) -> MdbResult<bool> {
        let mut lock = self.collections.lock().expect("mutex poisoned");
        if let Some(docs) = lock.get_mut(ns) {
            if let Some(idx) = docs.iter().position(|doc| document_matches(doc, filter)) {
                docs.remove(idx);
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn delete_many(&self, ns: &str, filter: &Document) -> MdbResult<u64> {
        let mut lock = self.collections.lock().expect("mutex poisoned");
        if let Some(docs) = lock.get_mut(ns) {
            let before = docs.len();
            docs.retain(|doc| !document_matches(doc, filter));
            return Ok((before - docs.len()) as u64);
        }
        Ok(0)
    }

    fn update_one(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
    ) -> MdbResult<bool> {
        let mut lock = self.collections.lock().expect("mutex poisoned");
        if let Some(docs) = lock.get_mut(ns) {
            if let Some(doc) = docs.iter_mut().find(|d| document_matches(d, filter)) {
                apply_update(doc, update)?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// 简单的文档匹配：filter 中每个字段都必须在 doc 中存在且相等。
fn document_matches(doc: &Document, filter: &Document) -> bool {
    if filter.is_empty() {
        return true;
    }
    for (key, filter_val) in filter.iter() {
        match doc.get(key) {
            Some(doc_val) if doc_val == filter_val => {}
            _ => return false,
        }
    }
    true
}

/// 简单的 $set 更新：`{ "$set": { "field": value, ... } }`。
fn apply_update(doc: &mut Document, update: &Document) -> MdbResult<()> {
    if let Some(Bson::Document(set_doc)) = update.get("$set") {
        for (key, value) in set_doc.iter() {
            doc.set(key, value.clone());
        }
        return Ok(());
    }
    Err(MdbError::new(
        ErrorCode::FailedToParse,
        "update document must contain $set operator",
    ))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_bucket() -> GridFsBucket {
        let storage = Arc::new(InMemoryStorage::new());
        GridFsBucket::new("testdb", storage)
    }

    fn make_bucket_with_prefix(prefix: &str) -> GridFsBucket {
        let storage = Arc::new(InMemoryStorage::new());
        GridFsBucket::with_prefix("testdb", prefix, storage)
    }

    // -- chunker tests --

    #[test]
    fn chunk_data_empty() {
        let chunks = chunk_data(&[], 255);
        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn chunk_data_exact_multiple() {
        let data = vec![0xAA; 200];
        let chunks = chunk_data(&data, 100);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 100);
        assert_eq!(chunks[1].len(), 100);
        assert_eq!(chunks[0], vec![0xAA; 100]);
        assert_eq!(chunks[1], vec![0xAA; 100]);
    }

    #[test]
    fn chunk_data_remainder() {
        let data = vec![0xBB; 250];
        let chunks = chunk_data(&data, 100);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), 100);
        assert_eq!(chunks[1].len(), 100);
        assert_eq!(chunks[2].len(), 50);
    }

    #[test]
    fn chunk_data_single_byte_chunks() {
        let data = b"abc";
        let chunks = chunk_data(data, 1);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], b"a");
        assert_eq!(chunks[1], b"b");
        assert_eq!(chunks[2], b"c");
    }

    #[test]
    fn chunk_data_large_chunk() {
        let data = vec![0xCC; 10];
        let chunks = chunk_data(&data, 1024);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 10);
    }

    #[test]
    fn reconstruct_data_empty() {
        let result = reconstruct_data(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn reconstruct_data_single_chunk() {
        let result = reconstruct_data(vec![b"hello".to_vec()]);
        assert_eq!(result, b"hello");
    }

    #[test]
    fn reconstruct_data_multiple_chunks() {
        let chunks = vec![b"hel".to_vec(), b"lo ".to_vec(), b"world".to_vec()];
        let result = reconstruct_data(chunks);
        assert_eq!(result, b"hello world");
    }

    #[test]
    fn chunk_roundtrip() {
        let original = vec![0x42; 1000];
        let chunks = chunk_data(&original, 256);
        let reconstructed = reconstruct_data(chunks);
        assert_eq!(reconstructed, original);
    }

    #[test]
    fn test_reconstruct_from_binaries() {
        let chunks = vec![(0, b"ab" as &[u8]), (1, b"cd"), (2, b"ef")];
        let result = super::reconstruct_from_binaries(&chunks);
        assert_eq!(result, b"abcdef");
    }

    // -- GridFsFile tests --

    #[test]
    fn gridfs_file_to_document_roundtrip() {
        let file = GridFsFile {
            _id: ObjectId::new().unwrap(),
            length: 12345,
            chunk_size: 255 * 1024,
            filename: "test.txt".to_owned(),
            content_type: "text/plain".to_owned(),
            metadata: {
                let mut m = Document::new();
                m.insert("author", "tester");
                m
            },
            upload_date: 1_700_000_000_000,
        };

        let doc = file.to_document();
        let back = GridFsFile::from_document(&doc).unwrap();
        assert_eq!(back, file);
    }

    #[test]
    fn gridfs_file_from_document_missing_field() {
        let mut doc = Document::new();
        doc.insert("_id", ObjectId::new().unwrap());
        doc.insert("length", 100i64);
        // missing chunkSize
        assert!(GridFsFile::from_document(&doc).is_none());
    }

    // -- GridFsChunk tests --

    #[test]
    fn gridfs_chunk_to_document_roundtrip() {
        let chunk = GridFsChunk {
            _id: ObjectId::new().unwrap(),
            files_id: ObjectId::new().unwrap(),
            n: 0,
            data: vec![0x01, 0x02, 0x03],
        };

        let doc = chunk.to_document();
        let back = GridFsChunk::from_document(&doc).unwrap();
        assert_eq!(back, chunk);
    }

    // -- Bucket upload/download tests --

    #[test]
    fn upload_download_small_file() {
        let bucket = make_bucket();
        let data = b"Hello, GridFS!";
        let file_id = bucket
            .upload_from_bytes("hello.txt", data, None, None)
            .unwrap();

        let downloaded = bucket.download_to_bytes(file_id).unwrap();
        assert_eq!(downloaded, data);
    }

    #[test]
    fn upload_download_empty_file() {
        let bucket = make_bucket();
        let file_id = bucket
            .upload_from_bytes("empty.txt", b"", None, None)
            .unwrap();

        let downloaded = bucket.download_to_bytes(file_id).unwrap();
        assert!(downloaded.is_empty());
    }

    #[test]
    fn upload_download_large_file() {
        let bucket = make_bucket().with_chunk_size(64);
        let data: Vec<u8> = (0..500).map(|i| (i % 256) as u8).collect();
        let file_id = bucket
            .upload_from_bytes("large.bin", &data, Some("application/octet-stream"), None)
            .unwrap();

        let downloaded = bucket.download_to_bytes(file_id).unwrap();
        assert_eq!(downloaded, data);
    }

    #[test]
    fn upload_download_exact_chunk_size() {
        let bucket = make_bucket().with_chunk_size(100);
        let data = vec![0xAB; 100];
        let file_id = bucket
            .upload_from_bytes("exact.bin", &data, None, None)
            .unwrap();

        let downloaded = bucket.download_to_bytes(file_id).unwrap();
        assert_eq!(downloaded, data);
    }

    #[test]
    fn upload_download_with_metadata() {
        let bucket = make_bucket();
        let mut meta = Document::new();
        meta.insert("author", "test-user");
        meta.insert("version", 42i32);

        let file_id = bucket
            .upload_from_bytes("meta.txt", b"content", Some("text/plain"), Some(meta))
            .unwrap();

        let downloaded = bucket.download_to_bytes(file_id).unwrap();
        assert_eq!(downloaded, b"content");
    }

    #[test]
    fn find_by_filename() {
        let bucket = make_bucket();
        bucket
            .upload_from_bytes("a.txt", b"aaa", None, None)
            .unwrap();
        bucket
            .upload_from_bytes("b.txt", b"bbb", None, None)
            .unwrap();
        bucket
            .upload_from_bytes("a.txt", b"aaa2", None, None)
            .unwrap();

        let files = bucket.find_by_filename("a.txt").unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|f| f.filename == "a.txt"));
    }

    #[test]
    fn find_with_filter() {
        let bucket = make_bucket();
        bucket
            .upload_from_bytes("a.txt", b"a", None, None)
            .unwrap();
        bucket
            .upload_from_bytes("b.txt", b"b", None, None)
            .unwrap();

        let mut filter = Document::new();
        filter.insert("filename", "a.txt");
        let files = bucket.find(&filter).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "a.txt");
    }

    #[test]
    fn delete_file() {
        let bucket = make_bucket();
        let file_id = bucket
            .upload_from_bytes("delete_me.txt", b"bye", None, None)
            .unwrap();

        let deleted = bucket.delete(file_id).unwrap();
        assert!(deleted);

        // Attempting to download should fail
        let result = bucket.download_to_bytes(file_id);
        assert!(result.is_err());
    }

    #[test]
    fn delete_nonexistent_file() {
        let bucket = make_bucket();
        let fake_id = ObjectId::new().unwrap();
        let deleted = bucket.delete(fake_id).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn rename_file() {
        let bucket = make_bucket();
        let file_id = bucket
            .upload_from_bytes("old_name.txt", b"data", None, None)
            .unwrap();

        let renamed = bucket.rename(file_id, "new_name.txt").unwrap();
        assert!(renamed);

        let files = bucket.find_by_filename("new_name.txt").unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "new_name.txt");

        let old_files = bucket.find_by_filename("old_name.txt").unwrap();
        assert!(old_files.is_empty());
    }

    #[test]
    fn custom_prefix() {
        let bucket = make_bucket_with_prefix("uploads");
        let file_id = bucket
            .upload_from_bytes("doc.pdf", b"pdf-content", None, None)
            .unwrap();

        let downloaded = bucket.download_to_bytes(file_id).unwrap();
        assert_eq!(downloaded, b"pdf-content");
    }

    #[test]
    fn multiple_files_same_name() {
        let bucket = make_bucket();
        let id1 = bucket
            .upload_from_bytes("dup.txt", b"v1", None, None)
            .unwrap();
        let id2 = bucket
            .upload_from_bytes("dup.txt", b"v2", None, None)
            .unwrap();

        assert_ne!(id1, id2);
        assert_eq!(bucket.download_to_bytes(id1).unwrap(), b"v1");
        assert_eq!(bucket.download_to_bytes(id2).unwrap(), b"v2");
    }

    // -- Command handler tests --

    #[test]
    fn handle_upload_command() {
        let bucket = make_bucket();
        let handler = GridFsCommandHandler::new(bucket);

        let mut cmd = Document::new();
        cmd.insert("filename", "cmd_upload.txt");
        cmd.insert(
            "data",
            Bson::Binary(mdb_doc::Binary::generic(b"command data".to_vec())),
        );
        cmd.insert("contentType", "text/plain");

        let reply = handler.handle_upload(&cmd).unwrap();
        assert_eq!(reply.get("ok"), Some(&Bson::Double(1.0)));
        assert!(reply.get("_id").is_some());
    }

    #[test]
    fn handle_download_command() {
        let bucket = make_bucket();
        let file_id = bucket
            .upload_from_bytes("dl_test.txt", b"download me", None, None)
            .unwrap();

        let handler = GridFsCommandHandler::new(bucket);

        let mut cmd = Document::new();
        cmd.insert("_id", file_id);

        let reply = handler.handle_download(&cmd).unwrap();
        assert_eq!(reply.get("ok"), Some(&Bson::Double(1.0)));
        assert_eq!(reply.get("length"), Some(&Bson::Int64(11)));
        assert_eq!(
            reply.get("filename"),
            Some(&Bson::String("dl_test.txt".into()))
        );
    }

    #[test]
    fn handle_find_command() {
        let bucket = make_bucket();
        bucket
            .upload_from_bytes("find_me.txt", b"find", None, None)
            .unwrap();

        let handler = GridFsCommandHandler::new(bucket);

        let mut cmd = Document::new();
        let mut filter = Document::new();
        filter.insert("filename", "find_me.txt");
        cmd.insert("filter", Bson::Document(filter));

        let reply = handler.handle_find(&cmd).unwrap();
        assert_eq!(reply.get("ok"), Some(&Bson::Double(1.0)));
        match reply.get("files") {
            Some(Bson::Array(arr)) => assert_eq!(arr.len(), 1),
            _ => panic!("expected 'files' array"),
        }
    }

    #[test]
    fn handle_delete_command() {
        let bucket = make_bucket();
        let file_id = bucket
            .upload_from_bytes("del_cmd.txt", b"delete me", None, None)
            .unwrap();

        let handler = GridFsCommandHandler::new(bucket);

        let mut cmd = Document::new();
        cmd.insert("_id", file_id);

        let reply = handler.handle_delete(&cmd).unwrap();
        assert_eq!(reply.get("ok"), Some(&Bson::Double(1.0)));
        assert_eq!(reply.get("deleted"), Some(&Bson::Boolean(true)));
    }

    #[test]
    fn handle_rename_command() {
        let bucket = make_bucket();
        let file_id = bucket
            .upload_from_bytes("rename_cmd.txt", b"rename", None, None)
            .unwrap();

        let handler = GridFsCommandHandler::new(bucket);

        let mut cmd = Document::new();
        cmd.insert("_id", file_id);
        cmd.insert("new_filename", "renamed_cmd.txt");

        let reply = handler.handle_rename(&cmd).unwrap();
        assert_eq!(reply.get("ok"), Some(&Bson::Double(1.0)));
        assert_eq!(reply.get("renamed"), Some(&Bson::Boolean(true)));
    }

    #[test]
    fn dispatch_unknown_command() {
        let bucket = make_bucket();
        let handler = GridFsCommandHandler::new(bucket);

        let mut cmd = Document::new();
        cmd.insert("gridfs.unknown", Bson::Document(Document::new()));

        let result = handler.dispatch(&cmd);
        assert!(result.is_err());
    }

    // -- InMemoryStorage tests --

    #[test]
    fn in_memory_insert_and_find() {
        let storage = InMemoryStorage::new();
        let mut doc = Document::new();
        doc.insert("_id", ObjectId::new().unwrap());
        doc.insert("name", "test");

        let id = storage.insert_one("test.ns", doc).unwrap();

        let mut filter = Document::new();
        filter.insert("_id", id);
        let found = storage.find_one("test.ns", &filter).unwrap();
        assert!(found.is_some());
    }

    #[test]
    fn in_memory_find_many() {
        let storage = InMemoryStorage::new();
        for i in 0..5 {
            let mut doc = Document::new();
            doc.insert("_id", ObjectId::new().unwrap());
            doc.insert("value", i);
            storage.insert_one("test.ns", doc).unwrap();
        }

        let filter = Document::new();
        let results = storage.find_many("test.ns", &filter).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn in_memory_delete_one() {
        let storage = InMemoryStorage::new();
        let mut doc = Document::new();
        let id = ObjectId::new().unwrap();
        doc.insert("_id", id);
        storage.insert_one("test.ns", doc).unwrap();

        let mut filter = Document::new();
        filter.insert("_id", id);
        let deleted = storage.delete_one("test.ns", &filter).unwrap();
        assert!(deleted);
    }

    #[test]
    fn in_memory_update_one() {
        let storage = InMemoryStorage::new();
        let id = ObjectId::new().unwrap();
        let mut doc = Document::new();
        doc.insert("_id", id);
        doc.insert("name", "old");
        storage.insert_one("test.ns", doc).unwrap();

        let mut filter = Document::new();
        filter.insert("_id", id);
        let mut update = Document::new();
        let mut set = Document::new();
        set.insert("name", "new");
        update.insert("$set", Bson::Document(set));

        let updated = storage.update_one("test.ns", &filter, &update).unwrap();
        assert!(updated);

        let found = storage
            .find_one("test.ns", &filter)
            .unwrap()
            .unwrap();
        assert_eq!(found.get("name"), Some(&Bson::String("new".into())));
    }

    // -- Edge case tests --

    #[test]
    fn upload_single_byte_file() {
        let bucket = make_bucket();
        let file_id = bucket
            .upload_from_bytes("single.txt", &[0x42], None, None)
            .unwrap();
        let data = bucket.download_to_bytes(file_id).unwrap();
        assert_eq!(data, &[0x42]);
    }

    #[test]
    fn upload_two_chunk_size_file() {
        let bucket = make_bucket().with_chunk_size(10);
        let data = vec![0x55; 20];
        let file_id = bucket
            .upload_from_bytes("two_chunks.bin", &data, None, None)
            .unwrap();
        let downloaded = bucket.download_to_bytes(file_id).unwrap();
        assert_eq!(downloaded, data);
    }

    #[test]
    fn upload_one_over_chunk_size_file() {
        let bucket = make_bucket().with_chunk_size(10);
        let data = vec![0x77; 11];
        let file_id = bucket
            .upload_from_bytes("over.bin", &data, None, None)
            .unwrap();
        let downloaded = bucket.download_to_bytes(file_id).unwrap();
        assert_eq!(downloaded, data);
    }
}
