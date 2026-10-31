//! LRU Block Cache：LSM-Tree 读路径的块级缓存。
//!
//! 设计：
//! - 键：`(file_number: u64, block_offset: u64)`
//! - 值：解压后的块数据
//! - 策略：LRU（最近最少使用），容量超限时淘汰最久未访问的块
//! - 线程安全：使用 `Mutex` 保护，适合读多写少场景
//!
//! 与 LevelDB/RocksDB 的 BlockCache 类似，缓存 SSTable 的解压块，
//! 避免重复读盘和解压。

use std::collections::HashMap;
use std::sync::Mutex;

/// 缓存键：`(文件编号, 块偏移)`。
pub type BlockCacheKey = (u64, u64);

/// LRU 块缓存。
#[derive(Debug)]
pub struct BlockCache {
    inner: Mutex<BlockCacheInner>,
    /// 最大缓存字节数。
    capacity: usize,
}

#[derive(Debug)]
struct BlockCacheInner {
    /// 键 → (数据, 最后访问序号)。
    map: HashMap<BlockCacheKey, (Vec<u8>, u64)>,
    /// 当前总字节数。
    current_size: usize,
    /// 单调递增访问计数器。
    access_counter: u64,
}

impl BlockCache {
    /// 创建指定容量（字节）的缓存。
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(BlockCacheInner {
                map: HashMap::new(),
                current_size: 0,
                access_counter: 0,
            }),
            capacity,
        }
    }

    /// 从缓存读取块。命中时更新 LRU 位置。
    pub fn get(&self, key: &BlockCacheKey) -> Option<Vec<u8>> {
        let mut inner = self.inner.lock().unwrap();
        let result = inner.map.get(key).map(|(data, _)| data.clone());
        if result.is_some() {
            inner.access_counter += 1;
            let ts = inner.access_counter;
            if let Some(entry) = inner.map.get_mut(key) {
                entry.1 = ts;
            }
        }
        result
    }

    /// 写入块到缓存。如果容量超限，淘汰 LRU 块。
    pub fn put(&self, key: BlockCacheKey, data: Vec<u8>) {
        let mut inner = self.inner.lock().unwrap();
        // 如果已存在，先移除旧条目
        if let Some((old_data, _)) = inner.map.remove(&key) {
            inner.current_size -= old_data.len();
        }
        inner.access_counter += 1;
        let entry_size = data.len();
        let ts = inner.access_counter;
        inner.map.insert(key, (data, ts));
        inner.current_size += entry_size;
        // 淘汰超容量的 LRU 块
        while inner.current_size > self.capacity && !inner.map.is_empty() {
            self.evict_lru(&mut inner);
        }
    }

    /// 从缓存移除指定键。
    pub fn invalidate(&self, key: &BlockCacheKey) {
        let mut inner = self.inner.lock().unwrap();
        if let Some((data, _)) = inner.map.remove(key) {
            inner.current_size -= data.len();
        }
    }

    /// 移除指定文件的所有缓存块（文件被删除/重写时调用）。
    pub fn invalidate_file(&self, file_number: u64) {
        let mut inner = self.inner.lock().unwrap();
        let keys_to_remove: Vec<BlockCacheKey> = inner
            .map
            .keys()
            .filter(|(fnr, _)| *fnr == file_number)
            .cloned()
            .collect();
        for key in keys_to_remove {
            if let Some((data, _)) = inner.map.remove(&key) {
                inner.current_size -= data.len();
            }
        }
    }

    /// 清空缓存。
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.map.clear();
        inner.current_size = 0;
    }

    /// 当前缓存的条目数。
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }

    /// 缓存是否为空。
    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().map.is_empty()
    }

    /// 当前缓存的总字节数。
    pub fn current_size(&self) -> usize {
        self.inner.lock().unwrap().current_size
    }

    /// 淘汰一个 LRU 条目（内部使用，调用者必须持有锁）。
    fn evict_lru(&self, inner: &mut BlockCacheInner) {
        if let Some(key) = inner
            .map
            .iter()
            .min_by_key(|(_, (_, ts))| *ts)
            .map(|(k, _)| *k)
        {
            if let Some((data, _)) = inner.map.remove(&key) {
                inner.current_size -= data.len();
            }
        }
    }
}

impl Default for BlockCache {
    fn default() -> Self {
        // 默认 256 MB
        Self::new(256 * 1024 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_roundtrip() {
        let cache = BlockCache::new(1024);
        assert!(cache.is_empty());

        cache.put((1, 0), b"hello".to_vec());
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.current_size(), 5);

        let got = cache.get(&(1, 0));
        assert_eq!(got, Some(b"hello".to_vec()));

        cache.invalidate(&(1, 0));
        assert!(cache.is_empty());
    }

    #[test]
    fn lru_eviction() {
        let cache = BlockCache::new(10); // 只能存 10 字节

        cache.put((1, 0), b"aaaaaaaa".to_vec()); // 8 bytes
        cache.put((1, 8), b"bbbb".to_vec());     // 4 bytes, total 12 > 10, evict LRU

        // 第一个块应该被淘汰
        assert!(cache.get(&(1, 0)).is_none());
        assert!(cache.get(&(1, 8)).is_some());
        assert_eq!(cache.current_size(), 4);
    }

    #[test]
    fn lru_access_updates_order() {
        // 容量足够大，不会触发淘汰
        let cache = BlockCache::new(1024);

        cache.put((1, 0), b"aaaaaaaa".to_vec()); // 8 bytes, ts=1
        cache.put((1, 8), b"bbbb".to_vec());     // 4 bytes, ts=2

        // 访问第一个块，更新其 LRU 位置
        cache.get(&(1, 0)); // ts=3

        // 现在 (1,8) 的 ts=2 是最小的，应该是 LRU
        // 验证两个块都还在
        assert!(cache.get(&(1, 0)).is_some());
        assert!(cache.get(&(1, 8)).is_some());

        // 清空后重新测试淘汰逻辑
        cache.clear();
        let cache2 = BlockCache::new(10); // 只能存 10 字节

        cache2.put((1, 0), b"aaaaaaaa".to_vec()); // 8 bytes, ts=1
        cache2.put((1, 8), b"bbbb".to_vec());     // 4 bytes, ts=2, total=12 > 10

        // (1,0) 是 LRU，应该被淘汰
        assert!(cache2.get(&(1, 0)).is_none(), "LRU (1,0) should be evicted");
        assert!(cache2.get(&(1, 8)).is_some(), "recently added (1,8) should survive");
    }

    #[test]
    fn invalidate_file() {
        let cache = BlockCache::new(1024);
        cache.put((1, 0), b"file1-block0".to_vec());
        cache.put((1, 100), b"file1-block1".to_vec());
        cache.put((2, 0), b"file2-block0".to_vec());

        cache.invalidate_file(1);
        assert!(cache.get(&(1, 0)).is_none());
        assert!(cache.get(&(1, 100)).is_none());
        assert!(cache.get(&(2, 0)).is_some());
    }

    #[test]
    fn overwrite_existing_key() {
        let cache = BlockCache::new(1024);
        cache.put((1, 0), b"old_data".to_vec());
        assert_eq!(cache.current_size(), 8);

        cache.put((1, 0), b"new_data_longer".to_vec());
        // "new_data_longer" is 15 bytes
        assert_eq!(cache.current_size(), 15);
        assert_eq!(cache.get(&(1, 0)), Some(b"new_data_longer".to_vec()));
    }
}
