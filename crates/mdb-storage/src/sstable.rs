use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use mdb_common::MdbResult;
use mdb_crypto::crc32c;

use crate::bloom::BloomFilter;
use crate::compression::{
    wrap_compressed_block, unwrap_compressed_block, CompressionAlgorithm, Compressor,
};
use crate::error::*;
use crate::types::*;

#[derive(Debug)]
pub struct BlockBuilder {
    entries: Vec<u8>,
    restarts: Vec<u32>,
    restart_interval: usize,
    last_key: Vec<u8>,
    count: u16,
    seqno_max: u64,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        Self {
            entries: Vec::new(),
            restarts: Vec::new(),
            restart_interval,
            last_key: Vec::new(),
            count: 0,
            seqno_max: 0,
        }
    }

    pub fn add(&mut self, internal_key: &[u8], value: &[u8]) {
        let is_restart = self.count as usize % self.restart_interval == 0;
        if is_restart {
            self.restarts.push(self.entries.len() as u32);
        }

        let seqno = seqno_of(internal_key);
        if seqno > self.seqno_max {
            self.seqno_max = seqno;
        }

        let shared_len = if is_restart {
            0
        } else {
            let mut n = 0usize;
            let limit = self.last_key.len().min(internal_key.len());
            while n < limit && self.last_key[n] == internal_key[n] {
                n += 1;
            }
            n
        };

        let unshared_len = internal_key.len() - shared_len;

        let mut tmp = Vec::new();
        put_uvarint(&mut tmp, shared_len as u64);
        put_uvarint(&mut tmp, unshared_len as u64);
        put_uvarint(&mut tmp, value.len() as u64);

        self.entries.extend_from_slice(&tmp);
        self.entries.extend_from_slice(&internal_key[shared_len..]);
        self.entries.extend_from_slice(value);

        self.last_key = internal_key.to_vec();
        self.count += 1;
    }

    pub fn finish(mut self) -> (Vec<u8>, u64) {
        let restart_array_offset = (BLOCK_HEADER_SIZE + self.entries.len()) as u32;

        for r in &self.restarts {
            self.entries.extend_from_slice(&r.to_le_bytes());
        }

        let mut block = Vec::with_capacity(BLOCK_HEADER_SIZE + self.entries.len() + 4);
        block.push(0u8);
        block.extend_from_slice(&(self.restarts.len() as u16).to_le_bytes());
        block.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        block.extend_from_slice(&self.seqno_max.to_be_bytes());

        let crc_offset = block.len();
        block.extend_from_slice(&[0u8; 4]);

        block.extend_from_slice(&self.count.to_le_bytes());
        block.extend_from_slice(&(self.restarts.len() as u16).to_le_bytes());
        block.extend_from_slice(&restart_array_offset.to_le_bytes());
        block.extend_from_slice(&self.entries);

        let crc = {
            let mut c = mdb_crypto::crc32c::Crc32c::new();
            c.update(&block[..crc_offset]);
            c.update(&block[crc_offset + 4..]);
            c.finish()
        };
        block[crc_offset..crc_offset + 4].copy_from_slice(&crc.to_be_bytes());

        (block, self.seqno_max)
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn estimated_size(&self) -> usize {
        BLOCK_HEADER_SIZE + self.entries.len() + self.restarts.len() * 4
    }
}

#[derive(Debug, Clone)]
pub struct BlockEntry {
    pub internal_key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct BlockReader {
    data: Vec<u8>,
    #[allow(dead_code)]
    restarts: Vec<u32>,
    num_entries: u16,
}

impl BlockReader {
    pub fn parse(block: Vec<u8>) -> MdbResult<Self> {
        if block.len() < BLOCK_HEADER_SIZE {
            return Err(corrupt_err("block too small for header"));
        }

        let stored_crc = u32::from_be_bytes([block[15], block[16], block[17], block[18]]);
        let mut c = mdb_crypto::crc32c::Crc32c::new();
        c.update(&block[..15]);
        c.update(&block[19..]);
        let computed_crc = c.finish();
        if stored_crc != computed_crc {
            return Err(corrupt_err(format!(
                "block CRC: stored={:#x} computed={:#x}",
                stored_crc, computed_crc
            )));
        }

        let num_entries = u16::from_le_bytes([block[19], block[20]]);
        let restart_count = u16::from_le_bytes([block[1], block[2]]);
        let restart_array_offset = u32::from_le_bytes([
            block[23], block[24], block[25], block[26],
        ]) as usize;

        let mut restarts = Vec::with_capacity(restart_count as usize);
        for i in 0..restart_count as usize {
            let off = restart_array_offset + i * 4;
            if off + 4 > block.len() {
                return Err(corrupt_err("restart array out of bounds"));
            }
            restarts.push(u32::from_le_bytes([
                block[off], block[off + 1], block[off + 2], block[off + 3],
            ]));
        }

        Ok(Self { data: block, restarts, num_entries })
    }

    fn read_entries(&self) -> MdbResult<Vec<BlockEntry>> {
        let mut entries = Vec::with_capacity(self.num_entries as usize);
        let mut pos = BLOCK_HEADER_SIZE;
        let mut current_key = Vec::new();

        for _ in 0..self.num_entries {
            let (shared, n1) = read_uvarint(&self.data[pos..])
                .ok_or_else(|| corrupt_err("bad shared_len uvarint"))?;
            pos += n1;
            let (unshared, n2) = read_uvarint(&self.data[pos..])
                .ok_or_else(|| corrupt_err("bad unshared_len uvarint"))?;
            pos += n2;
            let (val_len, n3) = read_uvarint(&self.data[pos..])
                .ok_or_else(|| corrupt_err("bad value_len uvarint"))?;
            pos += n3;

            let shared = shared as usize;
            let unshared = unshared as usize;
            let val_len = val_len as usize;

            if pos + unshared + val_len > self.data.len() {
                return Err(corrupt_err("entry extends beyond block"));
            }

            current_key.truncate(shared);
            current_key.extend_from_slice(&self.data[pos..pos + unshared]);
            pos += unshared;

            let value = self.data[pos..pos + val_len].to_vec();
            pos += val_len;

            entries.push(BlockEntry {
                internal_key: current_key.clone(),
                value,
            });
        }
        Ok(entries)
    }

    pub fn entries(&self) -> MdbResult<Vec<BlockEntry>> {
        self.read_entries()
    }

    pub fn num_entries(&self) -> usize {
        self.num_entries as usize
    }

    pub fn seqno_max(&self) -> u64 {
        u64::from_be_bytes([
            self.data[7], self.data[8], self.data[9], self.data[10],
            self.data[11], self.data[12], self.data[13], self.data[14],
        ])
    }

    /// 在 block 内二分查找第一个用户键 `>= target`（原始键语义）的下标。
    pub fn find_first_not_before(&self, target: &[u8]) -> MdbResult<usize> {
        let entries = self.read_entries()?;
        let mut lo = 0usize;
        let mut hi = entries.len();

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match cmp_ik_user_key(&entries[mid].internal_key, target) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Ok(lo)
    }
}

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub last_key: Vec<u8>,
    pub block_offset: u64,
    pub block_length: u32,
    pub uncompressed_size: u32,
}

#[derive(Debug, Clone)]
pub struct Footer {
    pub magic: u64,
    pub format_version: u32,
    pub index_offset: u64,
    pub index_length: u32,
    pub filter_offset: u64,
    pub filter_length: u32,
    pub seqno_min: u64,
    pub seqno_max: u64,
    pub crc32c: u32,
}

impl Footer {
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..8].copy_from_slice(&self.magic.to_be_bytes());
        buf[8..12].copy_from_slice(&self.format_version.to_be_bytes());
        buf[12..20].copy_from_slice(&self.index_offset.to_be_bytes());
        buf[20..24].copy_from_slice(&self.index_length.to_be_bytes());
        buf[24..32].copy_from_slice(&self.filter_offset.to_be_bytes());
        buf[32..36].copy_from_slice(&self.filter_length.to_be_bytes());
        buf[36..44].copy_from_slice(&self.seqno_min.to_be_bytes());
        buf[44..52].copy_from_slice(&self.seqno_max.to_be_bytes());
        buf[52..56].copy_from_slice(&self.crc32c.to_be_bytes());
        buf
    }

    pub fn decode(data: &[u8; FOOTER_SIZE]) -> Self {
        Self {
            magic: u64::from_be_bytes(data[0..8].try_into().unwrap()),
            format_version: u32::from_be_bytes(data[8..12].try_into().unwrap()),
            index_offset: u64::from_be_bytes(data[12..20].try_into().unwrap()),
            index_length: u32::from_be_bytes(data[20..24].try_into().unwrap()),
            filter_offset: u64::from_be_bytes(data[24..32].try_into().unwrap()),
            filter_length: u32::from_be_bytes(data[32..36].try_into().unwrap()),
            seqno_min: u64::from_be_bytes(data[36..44].try_into().unwrap()),
            seqno_max: u64::from_be_bytes(data[44..52].try_into().unwrap()),
            crc32c: u32::from_be_bytes(data[52..56].try_into().unwrap()),
        }
    }

    pub fn verify_crc(&self) -> bool {
        let stored = self.crc32c;
        let mut f = self.clone();
        f.crc32c = 0;
        let encoded = f.encode();
        let computed = crc32c::crc32c(&encoded[..52]);
        stored == computed
    }
}

#[derive(Debug, Clone)]
pub struct SSTableMeta {
    pub file_number: u64,
    pub path: PathBuf,
    pub seqno_min: SequenceNumber,
    pub seqno_max: SequenceNumber,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
    pub level: usize,
}

#[derive(Debug)]
pub struct SSTableWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    block_size: usize,
    restart_interval: usize,
    builder: BlockBuilder,
    index_entries: Vec<IndexEntry>,
    bloom: BloomFilter,
    data_offset: u64,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    seqno_min: u64,
    seqno_max: u64,
    file_number: u64,
    compression: Box<dyn Compressor>,
    /// TDE 加密上下文：`Some` 时数据块在写入前加密。
    encryption: Option<EncryptionContext>,
}

/// TDE 加密上下文：持有集合名和加密管理器引用。
#[derive(Clone)]
pub struct EncryptionContext {
    pub collection: String,
    pub manager: Arc<crate::encryption::EncryptionManager>,
}

impl std::fmt::Debug for EncryptionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionContext")
            .field("collection", &self.collection)
            .field("manager", &"<EncryptionManager>")
            .finish()
    }
}

impl SSTableWriter {
    pub fn create(
        path: impl AsRef<Path>,
        file_number: u64,
        block_size_kb: u32,
        bloom_bits_per_key: u32,
        estimated_keys: usize,
    ) -> MdbResult<Self> {
        Self::create_with_compression(
            path,
            file_number,
            block_size_kb,
            bloom_bits_per_key,
            estimated_keys,
            CompressionAlgorithm::default(),
        )
    }

    pub fn create_with_compression(
        path: impl AsRef<Path>,
        file_number: u64,
        block_size_kb: u32,
        bloom_bits_per_key: u32,
        estimated_keys: usize,
        compression: CompressionAlgorithm,
    ) -> MdbResult<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path).map_err(|e| io_err(format!("create SST: {e}")))?;
        let block_size = block_size_kb as usize * 1024;

        Ok(Self {
            writer: BufWriter::new(file),
            path,
            block_size,
            restart_interval: RESTART_INTERVAL,
            builder: BlockBuilder::new(RESTART_INTERVAL),
            index_entries: Vec::new(),
            bloom: BloomFilter::new(estimated_keys.max(1) as u32, bloom_bits_per_key),
            data_offset: 0,
            first_key: Vec::new(),
            last_key: Vec::new(),
            seqno_min: u64::MAX,
            seqno_max: 0,
            file_number,
            compression: compression.create_compressor(),
            encryption: None,
        })
    }

    /// 创建带 TDE 加密的 SSTable 写入器。
    pub fn create_with_encryption(
        path: impl AsRef<Path>,
        file_number: u64,
        block_size_kb: u32,
        bloom_bits_per_key: u32,
        estimated_keys: usize,
        compression: CompressionAlgorithm,
        encryption: EncryptionContext,
    ) -> MdbResult<Self> {
        let mut writer = Self::create_with_compression(
            path, file_number, block_size_kb, bloom_bits_per_key, estimated_keys, compression,
        )?;
        writer.encryption = Some(encryption);
        Ok(writer)
    }

    pub fn add(&mut self, internal_key: &[u8], value: &[u8]) -> MdbResult<()> {
        let seqno = seqno_of(internal_key);
        if seqno < self.seqno_min {
            self.seqno_min = seqno;
        }
        if seqno > self.seqno_max {
            self.seqno_max = seqno;
        }

        if self.first_key.is_empty() {
            self.first_key = internal_key.to_vec();
        }
        self.last_key = internal_key.to_vec();

        // 布隆过滤器按**原始**用户键建，查询侧才能用原始键探测。
        // 早期版本误用转义后的键，会让含 0x00/0x01 的键永远探测不到。
        self.bloom.add(&user_key_of_owned(internal_key));

        self.builder.add(internal_key, value);

        if self.builder.estimated_size() >= self.block_size {
            self.flush_block()?;
        }

        Ok(())
    }

    fn flush_block(&mut self) -> MdbResult<()> {
        if self.builder.is_empty() {
            return Ok(());
        }
        let (block, _) = std::mem::replace(&mut self.builder, BlockBuilder::new(self.restart_interval)).finish();
        let uncompressed_size = block.len() as u32;

        let mut on_disk_data = if self.compression.name() == "none" {
            block
        } else {
            let compressed = self.compression.compress(&block);
            wrap_compressed_block(compressed)
        };

        // TDE：加密数据块（在压缩之后、写入之前）
        if let Some(ctx) = &self.encryption {
            on_disk_data = ctx.manager
                .encrypt_block(&ctx.collection, self.file_number as u32, self.data_offset, &on_disk_data)
                .map_err(|e| crate::error::io_err(format!("encrypt block: {e}")))?;
        }

        let block_len = on_disk_data.len() as u32;
        self.writer.write_all(&on_disk_data)
            .map_err(|e| io_err(format!("write block: {e}")))?;
        self.index_entries.push(IndexEntry {
            last_key: self.last_key.clone(),
            block_offset: self.data_offset,
            block_length: block_len,
            uncompressed_size,
        });
        self.data_offset += block_len as u64;
        Ok(())
    }

    pub fn finish(mut self) -> MdbResult<SSTableMeta> {
        self.flush_block()?;

        let index_offset = self.data_offset;
        let mut index_buf = Vec::new();
        index_buf.extend_from_slice(&(self.index_entries.len() as u32).to_be_bytes());
        for entry in &self.index_entries {
            index_buf.extend_from_slice(&(entry.last_key.len() as u32).to_be_bytes());
            index_buf.extend_from_slice(&entry.last_key);
            index_buf.extend_from_slice(&entry.block_offset.to_be_bytes());
            index_buf.extend_from_slice(&entry.block_length.to_be_bytes());
            index_buf.extend_from_slice(&entry.uncompressed_size.to_be_bytes());
        }
        let index_crc = crc32c::crc32c(&index_buf);
        index_buf.extend_from_slice(&index_crc.to_be_bytes());
        let index_length = index_buf.len() as u32;
        self.writer.write_all(&index_buf)
            .map_err(|e| io_err(format!("write index: {e}")))?;

        let filter_offset = self.data_offset + index_length as u64;
        let filter_data = self.bloom.serialize();
        let filter_length = filter_data.len() as u32;
        self.writer.write_all(&filter_data)
            .map_err(|e| io_err(format!("write filter: {e}")))?;

        let compression_id = self.compression.name().as_bytes();
        let mut comp_tag = [0u8; 8];
        comp_tag[..compression_id.len().min(8)].copy_from_slice(&compression_id[..compression_id.len().min(8)]);

        let footer = Footer {
            magic: SST_MAGIC,
            format_version: SST_FORMAT_VERSION,
            index_offset,
            index_length,
            filter_offset,
            filter_length,
            seqno_min: self.seqno_min,
            seqno_max: self.seqno_max,
            crc32c: 0,
        };
        let mut footer_buf = footer.encode();
        let footer_crc = crc32c::crc32c(&footer_buf[..52]);
        footer_buf[52..56].copy_from_slice(&footer_crc.to_be_bytes());
        self.writer.write_all(&footer_buf)
            .map_err(|e| io_err(format!("write footer: {e}")))?;
        self.writer.flush().map_err(|e| io_err(format!("flush SST: {e}")))?;

        let file = self.writer.into_inner().map_err(|e| io_err(format!("into_inner: {e}")))?;
        file.sync_all().map_err(|e| io_err(format!("fsync SST: {e}")))?;

        Ok(SSTableMeta {
            file_number: self.file_number,
            path: self.path,
            seqno_min: self.seqno_min,
            seqno_max: self.seqno_max,
            first_key: self.first_key,
            last_key: self.last_key,
            level: 0,
        })
    }
}

#[derive(Debug)]
pub struct SSTableReader {
    file: File,
    footer: Footer,
    meta: SSTableMeta,
    index: Vec<IndexEntry>,
    bloom: Option<BloomFilter>,
    compression: Box<dyn Compressor>,
    /// TDE 加密上下文：`Some` 时数据块在读取后解密。
    encryption: Option<EncryptionContext>,
    /// LRU 块缓存：`Some` 时读取的块会被缓存。
    cache: Option<Arc<crate::block_cache::BlockCache>>,
}

impl SSTableReader {
    pub fn open(path: impl AsRef<Path>, meta: SSTableMeta) -> MdbResult<Self> {
        Self::open_with_compression(path, meta, CompressionAlgorithm::default())
    }

    pub fn open_with_compression(
        path: impl AsRef<Path>,
        meta: SSTableMeta,
        compression: CompressionAlgorithm,
    ) -> MdbResult<Self> {
        let path = path.as_ref();
        let mut file = File::open(path).map_err(|e| io_err(format!("open SST: {e}")))?;

        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))
            .map_err(|e| io_err(format!("seek footer: {e}")))?;

        let mut footer_buf = [0u8; FOOTER_SIZE];
        file.read_exact(&mut footer_buf)
            .map_err(|e| io_err(format!("read footer: {e}")))?;

        let footer = Footer::decode(&footer_buf);
        if footer.magic != SST_MAGIC {
            return Err(corrupt_err(format!(
                "bad SST magic: {:#x}, expected {:#x}",
                footer.magic, SST_MAGIC
            )));
        }
        if !footer.verify_crc() {
            return Err(corrupt_err("SST footer CRC mismatch"));
        }

        file.seek(SeekFrom::Start(footer.index_offset))
            .map_err(|e| io_err(format!("seek index: {e}")))?;
        let mut index_buf = vec![0u8; footer.index_length as usize];
        file.read_exact(&mut index_buf)
            .map_err(|e| io_err(format!("read index: {e}")))?;

        let index_crc_stored = u32::from_be_bytes([
            index_buf[index_buf.len() - 4],
            index_buf[index_buf.len() - 3],
            index_buf[index_buf.len() - 2],
            index_buf[index_buf.len() - 1],
        ]);
        let index_crc_computed = crc32c::crc32c(&index_buf[..index_buf.len() - 4]);
        if index_crc_stored != index_crc_computed {
            return Err(corrupt_err("SST index CRC mismatch"));
        }

        let index = Self::parse_index(&index_buf[..index_buf.len() - 4])?;

        let bloom = if footer.filter_length > 0 {
            file.seek(SeekFrom::Start(footer.filter_offset))
                .map_err(|e| io_err(format!("seek filter: {e}")))?;
            let mut filter_buf = vec![0u8; footer.filter_length as usize];
            file.read_exact(&mut filter_buf)
                .map_err(|e| io_err(format!("read filter: {e}")))?;
            BloomFilter::deserialize(&filter_buf)
        } else {
            None
        };

        Ok(Self {
            file,
            footer,
            meta,
            index,
            bloom,
            compression: compression.create_compressor(),
            encryption: None,
            cache: None,
        })
    }

    /// 打开带 TDE 加密的 SSTable 读取器。
    pub fn open_with_encryption(
        path: impl AsRef<Path>,
        meta: SSTableMeta,
        compression: CompressionAlgorithm,
        encryption: EncryptionContext,
    ) -> MdbResult<Self> {
        let mut reader = Self::open_with_compression(path, meta, compression)?;
        reader.encryption = Some(encryption);
        Ok(reader)
    }

    /// 设置 LRU 块缓存。
    pub fn with_cache(mut self, cache: Arc<crate::block_cache::BlockCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    fn parse_index(data: &[u8]) -> MdbResult<Vec<IndexEntry>> {
        if data.len() < 4 {
            return Err(corrupt_err("index block too small"));
        }
        let num = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        let mut pos = 4;
        let mut entries = Vec::with_capacity(num);
        let has_uncompressed_size = data.len() >= 4 + num * (4 + 8 + 4 + 4);
        for _ in 0..num {
            if pos + 4 > data.len() {
                return Err(corrupt_err("index entry truncated (key_len)"));
            }
            let key_len = u32::from_be_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3],
            ]) as usize;
            pos += 4;
            if pos + key_len + 8 + 4 > data.len() {
                return Err(corrupt_err("index entry truncated"));
            }
            let last_key = data[pos..pos + key_len].to_vec();
            pos += key_len;
            let block_offset = u64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let block_length = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let uncompressed_size = if has_uncompressed_size && pos + 4 <= data.len() {
                u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap())
            } else {
                block_length
            };
            if has_uncompressed_size && pos + 4 <= data.len() {
                pos += 4;
            }
            entries.push(IndexEntry {
                last_key,
                block_offset,
                block_length,
                uncompressed_size,
            });
        }
        Ok(entries)
    }

    fn read_block_data(&mut self, offset: u64, length: u32, uncompressed_size: u32) -> MdbResult<Vec<u8>> {
        let cache_key = (self.meta.file_number, offset);

        // 先查缓存
        if let Some(ref cache) = self.cache {
            if let Some(cached) = cache.get(&cache_key) {
                return Ok(cached);
            }
        }

        self.file.seek(SeekFrom::Start(offset))
            .map_err(|e| io_err(format!("seek block: {e}")))?;
        let mut buf = vec![0u8; length as usize];
        self.file.read_exact(&mut buf)
            .map_err(|e| io_err(format!("read block: {e}")))?;

        // TDE：解密数据块（在解压缩之前）
        if let Some(ctx) = &self.encryption {
            buf = ctx.manager
                .decrypt_block(&ctx.collection, self.meta.file_number as u32, offset, &buf)
                .map_err(|e| crate::error::io_err(format!("decrypt block: {e}")))?;
        }

        let result = if self.compression.name() == "none" {
            buf
        } else {
            let compressed = unwrap_compressed_block(&buf, uncompressed_size as usize)?;
            self.compression.decompress(&compressed, uncompressed_size as usize)?
        };

        // 写入缓存
        if let Some(ref cache) = self.cache {
            cache.put(cache_key, result.clone());
        }

        Ok(result)
    }

    pub fn get(&mut self, user_key: &[u8], snapshot_seqno: SequenceNumber) -> MdbResult<Option<Option<Vec<u8>>>> {
        if let Some(ref bloom) = self.bloom {
            if !bloom.contains(user_key) {
                return Ok(None);
            }
        }

        let start_block = self.find_block(user_key)?;
        let mut best: Option<Option<Vec<u8>>> = None;
        let mut best_seqno: Option<SequenceNumber> = None;

        for block_idx in start_block..self.index.len() {
            let (block_offset, block_length, uncompressed_size, last_key_clone) = {
                let entry = &self.index[block_idx];
                (entry.block_offset, entry.block_length, entry.uncompressed_size, entry.last_key.clone())
            };
            let block_data = self.read_block_data(block_offset, block_length, uncompressed_size)?;
            let reader = BlockReader::parse(block_data)?;
            let entries = reader.entries()?;

            let mut found_in_block = false;
            for block_entry in &entries {
                if let Some(sn) = parse_internal_key(&block_entry.internal_key).map(|(_, sn)| sn) {
                    if ik_user_key_eq(&block_entry.internal_key, user_key) {
                        found_in_block = true;
                        if sn <= snapshot_seqno && best_seqno.map_or(true, |bs| sn > bs) {
                            best_seqno = Some(sn);
                            best = Some(if block_entry.value.is_empty() {
                                None
                            } else {
                                Some(block_entry.value.clone())
                            });
                        }
                    } else if found_in_block {
                        break;
                    }
                }
            }
            if found_in_block {
                if cmp_ik_user_key(&last_key_clone, user_key) == std::cmp::Ordering::Greater {
                    break;
                }
            }
        }
        Ok(best)
    }

    pub fn range_scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        snapshot_seqno: SequenceNumber,
    ) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;

        let start_block = self.find_block(start)?;
        for block_idx in start_block..self.index.len() {
            let idx_entry = &self.index[block_idx];
            let block_data = self.read_block_data(idx_entry.block_offset, idx_entry.block_length, idx_entry.uncompressed_size)?;
            let reader = BlockReader::parse(block_data)?;
            let entries = reader.entries()?;

            for block_entry in &entries {
                if let Some((uk, sn)) = parse_internal_key_owned(&block_entry.internal_key) {
                    if uk.as_slice() >= end {
                        return Ok(results);
                    }
                    if uk.as_slice() < start {
                        continue;
                    }
                    if sn > snapshot_seqno {
                        continue;
                    }
                    if let Some(ref last) = last_user_key {
                        if uk == last.as_slice() {
                            continue;
                        }
                    }
                    last_user_key = Some(uk.clone());
                    if block_entry.value.is_empty() {
                        // 墓碑：同文件内较新版本删除该键，应从结果里移除。
                        results.retain(|(k, _)| k != &uk);
                        continue;
                    }
                    results.push((uk, block_entry.value.clone()));
                }
            }
        }
        Ok(results)
    }

    /// 无上界扫描：返回 `[start, +inf)` 区间内的全部键值对。
    ///
    /// 键长度无上界，有限 `\xff...` 哨兵会漏掉长键，全量扫描必须走这里。
    pub fn scan_all(&mut self, start: &[u8], snapshot_seqno: SequenceNumber) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;

        let start_block = self.find_block(start)?;
        for block_idx in start_block..self.index.len() {
            let idx_entry = &self.index[block_idx];
            let block_data = self.read_block_data(idx_entry.block_offset, idx_entry.block_length, idx_entry.uncompressed_size)?;
            let reader = BlockReader::parse(block_data)?;
            let entries = reader.entries()?;

            for block_entry in &entries {
                if let Some((uk, sn)) = parse_internal_key_owned(&block_entry.internal_key) {
                    if uk.as_slice() < start {
                        continue;
                    }
                    if sn > snapshot_seqno {
                        continue;
                    }
                    if let Some(ref last) = last_user_key {
                        if uk == last.as_slice() {
                            continue;
                        }
                    }
                    last_user_key = Some(uk.clone());
                    if block_entry.value.is_empty() {
                        // 墓碑：同文件内较新版本删除该键，应从结果里移除。
                        results.retain(|(k, _)| k != &uk);
                        continue;
                    }
                    results.push((uk, block_entry.value.clone()));
                }
            }
        }
        Ok(results)
    }

    fn find_block(&self, user_key: &[u8]) -> MdbResult<usize> {
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let uk = user_key_of(&self.index[mid].last_key);
            match uk.cmp(&escape_user_key(user_key)) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Ok(lo)
    }

    pub fn meta(&self) -> &SSTableMeta {
        &self.meta
    }

    pub fn seqno_max(&self) -> SequenceNumber {
        self.footer.seqno_max
    }

    pub fn seqno_min(&self) -> SequenceNumber {
        self.footer.seqno_min
    }

    pub fn might_contain(&self, user_key: &[u8]) -> bool {
        match &self.bloom {
            Some(b) => b.contains(user_key),
            None => true,
        }
    }

    pub fn overlaps(&self, start: &[u8], end: &[u8]) -> bool {
        if self.meta.first_key.is_empty() || self.meta.last_key.is_empty() {
            return false;
        }
        let fk = user_key_of(&self.meta.first_key);
        let lk = user_key_of(&self.meta.last_key);
        let fs = escape_user_key(start);
        let fe = escape_user_key(end);
        fk < &fe && lk >= &fs
    }

    pub fn iter_all_entries(&mut self) -> MdbResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        for block_idx in 0..self.index.len() {
            let idx_entry = &self.index[block_idx];
            let block_data = self.read_block_data(idx_entry.block_offset, idx_entry.block_length, idx_entry.uncompressed_size)?;
            let reader = BlockReader::parse(block_data)?;
            let entries = reader.entries()?;
            for block_entry in &entries {
                results.push((block_entry.internal_key.clone(), block_entry.value.clone()));
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = PathBuf::from(format!("test_sst_{}_{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn block_roundtrip() {
        let mut builder = BlockBuilder::new(16);
        let ik1 = make_internal_key(b"alpha", 10);
        let ik2 = make_internal_key(b"alpha", 5);
        let ik3 = make_internal_key(b"beta", 8);
        builder.add(&ik1, b"val1");
        builder.add(&ik2, b"val2");
        builder.add(&ik3, b"val3");

        let (block, _) = builder.finish();
        let reader = BlockReader::parse(block).unwrap();
        let entries = reader.entries().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(user_key_of(&entries[0].internal_key), b"alpha");
        assert_eq!(user_key_of(&entries[2].internal_key), b"beta");
    }

    #[test]
    fn footer_roundtrip() {
        let footer = Footer {
            magic: SST_MAGIC,
            format_version: 1,
            index_offset: 100,
            index_length: 200,
            filter_offset: 300,
            filter_length: 64,
            seqno_min: 1,
            seqno_max: 100,
            crc32c: 0,
        };
        let encoded = footer.encode();
        let decoded = Footer::decode(&encoded);
        assert_eq!(decoded.magic, SST_MAGIC);
        assert_eq!(decoded.index_offset, 100);
        assert_eq!(decoded.seqno_max, 100);
    }

    #[test]
    fn sst_write_and_read() {
        let dir = temp_dir();
        let path = dir.join("test.sst");

        let meta = {
            let mut writer = SSTableWriter::create(&path, 1, 4, 10, 100).unwrap();
            writer.add(&make_internal_key(b"aaa", 1), b"val_a").unwrap();
            writer.add(&make_internal_key(b"bbb", 2), b"val_b").unwrap();
            writer.add(&make_internal_key(b"ccc", 3), b"val_c").unwrap();
            writer.finish().unwrap()
        };

        let mut reader = SSTableReader::open(&path, meta.clone()).unwrap();
        assert_eq!(reader.get(b"bbb", 10).unwrap(), Some(Some(b"val_b".to_vec())));
        assert_eq!(reader.get(b"aaa", 10).unwrap(), Some(Some(b"val_a".to_vec())));
        assert_eq!(reader.get(b"ccc", 10).unwrap(), Some(Some(b"val_c".to_vec())));
        assert_eq!(reader.get(b"ddd", 10).unwrap(), None);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sst_snapshot_visibility() {
        let dir = temp_dir();
        let path = dir.join("snap.sst");

        let meta = {
            let mut writer = SSTableWriter::create(&path, 1, 4, 10, 100).unwrap();
            writer.add(&make_internal_key(b"key", 5), b"v5").unwrap();
            writer.add(&make_internal_key(b"key", 10), b"v10").unwrap();
            writer.finish().unwrap()
        };

        let mut reader = SSTableReader::open(&path, meta).unwrap();
        assert_eq!(reader.get(b"key", 10).unwrap(), Some(Some(b"v10".to_vec())));
        assert_eq!(reader.get(b"key", 7).unwrap(), Some(Some(b"v5".to_vec())));
        assert_eq!(reader.get(b"key", 3).unwrap(), None);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sst_tombstone() {
        let dir = temp_dir();
        let path = dir.join("tomb.sst");

        let meta = {
            let mut writer = SSTableWriter::create(&path, 1, 4, 10, 100).unwrap();
            writer.add(&make_internal_key(b"key", 1), b"val").unwrap();
            writer.add(&make_internal_key(b"key", 2), b"").unwrap();
            writer.finish().unwrap()
        };

        let mut reader = SSTableReader::open(&path, meta).unwrap();
        assert_eq!(reader.get(b"key", 10).unwrap(), Some(None));
        assert_eq!(reader.get(b"key", 1).unwrap(), Some(Some(b"val".to_vec())));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sst_bloom_filter_negative() {
        let dir = temp_dir();
        let path = dir.join("bloom.sst");

        let meta = {
            let mut writer = SSTableWriter::create(&path, 1, 4, 10, 100).unwrap();
            writer.add(&make_internal_key(b"present", 1), b"yes").unwrap();
            writer.finish().unwrap()
        };

        let reader = SSTableReader::open(&path, meta).unwrap();
        assert!(!reader.might_contain(b"absent"));
        assert!(reader.might_contain(b"present"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sst_range_scan() {
        let dir = temp_dir();
        let path = dir.join("scan.sst");

        let meta = {
            let mut writer = SSTableWriter::create(&path, 1, 4, 10, 100).unwrap();
            for i in 0..10u64 {
                let key = format!("key_{:04}", i);
                writer.add(&make_internal_key(key.as_bytes(), i), b"v").unwrap();
            }
            writer.finish().unwrap()
        };

        let mut reader = SSTableReader::open(&path, meta).unwrap();
        let results = reader.range_scan(b"key_0002", b"key_0006", 100).unwrap();
        assert_eq!(results.len(), 4);
        assert_eq!(results[0].0, b"key_0002");
        assert_eq!(results[3].0, b"key_0005");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn block_prefix_compression_reduces_size() {
        let mut builder = BlockBuilder::new(16);
        for i in 0..20u64 {
            let key = format!("key_{:04}", i);
            let ik = make_internal_key(key.as_bytes(), i);
            builder.add(&ik, b"v");
        }
        let (block, _) = builder.finish();
        let uncompressed_estimate = 20 * (4 + 10 + 1 + 8 + 1);
        assert!(block.len() < uncompressed_estimate, "prefix compression should reduce size: {} < {}", block.len(), uncompressed_estimate);
    }

    #[test]
    fn sst_many_keys() {
        let dir = temp_dir();
        let path = dir.join("many.sst");

        let meta = {
            let mut writer = SSTableWriter::create(&path, 1, 4, 10, 500).unwrap();
            for i in 0..500u64 {
                let key = format!("key_{:06}", i);
                let val = format!("val_{:06}", i);
                writer.add(&make_internal_key(key.as_bytes(), i), val.as_bytes()).unwrap();
            }
            writer.finish().unwrap()
        };

        let mut reader = SSTableReader::open(&path, meta).unwrap();
        for i in 0..500u64 {
            let key = format!("key_{:06}", i);
            let val = format!("val_{:06}", i);
            assert_eq!(
                reader.get(key.as_bytes(), 1000).unwrap(),
                Some(Some(val.into_bytes())),
                "failed for key_{i:06}"
            );
        }

        let _ = fs::remove_dir_all(&dir);
    }
}
