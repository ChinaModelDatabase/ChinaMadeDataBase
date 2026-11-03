use std::collections::BTreeMap;

use crate::types::*;

#[derive(Debug)]
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
    size_bytes: usize,
    max_size: usize,
    min_seqno: SequenceNumber,
    max_seqno: SequenceNumber,
}

impl MemTable {
    pub fn new(max_size: usize) -> Self {
        Self {
            map: BTreeMap::new(),
            size_bytes: 0,
            max_size,
            min_seqno: SequenceNumber::MAX,
            max_seqno: 0,
        }
    }

    pub fn get(&self, user_key: &[u8], snapshot_seqno: SequenceNumber) -> Option<Option<Vec<u8>>> {
        let lo = make_internal_key(user_key, snapshot_seqno);
        let hi = make_internal_key(user_key, 0);

        // 只取该 user_key 的最新可见版本：internal key 按 (user_key, seqno 降序)
        // 排列，命中的第一条即答案，不需要遍历后续条目。
        let Some((ik, val)) = self.map.range::<Vec<u8>, _>(lo..=hi).next() else {
            return None;
        };
        if let Some(sn) = parse_internal_key(ik).map(|(_, sn)| sn) {
            if ik_user_key_eq(ik, user_key) && sn <= snapshot_seqno {
                if val.is_empty() {
                    return Some(None);
                }
                return Some(Some(val.clone()));
            }
        }
        None
    }

    pub fn put(&mut self, user_key: Vec<u8>, seqno: SequenceNumber, value: Vec<u8>) {
        let ik = make_internal_key(&user_key, seqno);
        let entry_size = user_key.len() + value.len() + 16;
        self.map.insert(ik, value);
        self.size_bytes += entry_size;
        if seqno < self.min_seqno {
            self.min_seqno = seqno;
        }
        if seqno > self.max_seqno {
            self.max_seqno = seqno;
        }
    }

    pub fn delete(&mut self, user_key: Vec<u8>, seqno: SequenceNumber) {
        let ik = make_internal_key(&user_key, seqno);
        let entry_size = user_key.len() + 16;
        self.map.insert(ik, Vec::new());
        self.size_bytes += entry_size;
        if seqno < self.min_seqno {
            self.min_seqno = seqno;
        }
        if seqno > self.max_seqno {
            self.max_seqno = seqno;
        }
    }

    pub fn range_scan(
        &self,
        start: &[u8],
        end: &[u8],
        snapshot_seqno: SequenceNumber,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut results = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;

        let lo = make_internal_key(start, snapshot_seqno);
        let hi = make_internal_key(end, 0);

        for (ik, val) in self.map.range::<Vec<u8>, _>(lo..hi) {
            if let Some((uk, sn)) = parse_internal_key_owned(ik) {
                if sn > snapshot_seqno {
                    continue;
                }
                if let Some(ref last) = last_user_key {
                    if uk == last.as_slice() {
                        continue;
                    }
                }
                last_user_key = Some(uk.clone());
                if val.is_empty() {
                    // 墓碑：同层内较新版本删除该键。
                    results.retain(|(k, _)| k != &uk);
                    continue;
                }
                results.push((uk, val.clone()));
            }
        }
        results
    }

    /// 无上界扫描：返回 `[start, +inf)` 区间内的全部键值对。
    ///
    /// 键长度无上界，用有限的 `\xff...` 哨兵作上界会漏掉长键，
    /// 因此全量扫描必须走这个无界入口。
    pub fn scan_all(&self, start: &[u8], snapshot_seqno: SequenceNumber) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut results = Vec::new();
        let mut last_user_key: Option<Vec<u8>> = None;
        let lo = make_internal_key(start, snapshot_seqno);

        for (ik, val) in self.map.range::<Vec<u8>, _>(lo..) {
            if let Some((uk, sn)) = parse_internal_key_owned(ik) {
                if sn > snapshot_seqno {
                    continue;
                }
                if let Some(ref last) = last_user_key {
                    if uk == last.as_slice() {
                        continue;
                    }
                }
                last_user_key = Some(uk.clone());
                if val.is_empty() {
                    results.retain(|(k, _)| k != &uk);
                    continue;
                }
                results.push((uk, val.clone()));
            }
        }
        results
    }

    pub fn is_full(&self) -> bool {
        self.size_bytes >= self.max_size
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn min_seqno(&self) -> SequenceNumber {
        self.min_seqno
    }

    pub fn max_seqno(&self) -> SequenceNumber {
        self.max_seqno
    }

    pub fn freeze(self) -> FrozenMemTable {
        let min = self.min_seqno;
        let max = self.max_seqno;
        FrozenMemTable { map: self.map, min_seqno: min, max_seqno: max }
    }

    pub fn into_entries(self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        self.map
    }
}

#[derive(Debug, Clone)]
pub struct FrozenMemTable {
    pub map: BTreeMap<Vec<u8>, Vec<u8>>,
    pub min_seqno: SequenceNumber,
    pub max_seqno: SequenceNumber,
}

impl FrozenMemTable {
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], SequenceNumber, &[u8])> {
        self.map.iter().filter_map(|(ik, v)| {
            parse_internal_key(ik).map(|(uk, sn)| (uk, sn, v.as_slice()))
        })
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_memtable() -> MemTable {
        MemTable::new(1024 * 1024)
    }

    #[test]
    fn put_and_get() {
        let mut mt = make_memtable();
        mt.put(b"key1".to_vec(), 1, b"val1".to_vec());
        assert_eq!(mt.get(b"key1", 10), Some(Some(b"val1".to_vec())));
    }

    #[test]
    fn get_respects_snapshot() {
        let mut mt = make_memtable();
        mt.put(b"key1".to_vec(), 5, b"v1".to_vec());
        mt.put(b"key1".to_vec(), 10, b"v2".to_vec());
        assert_eq!(mt.get(b"key1", 10), Some(Some(b"v2".to_vec())));
        assert_eq!(mt.get(b"key1", 7), Some(Some(b"v1".to_vec())));
        assert_eq!(mt.get(b"key1", 3), None);
    }

    #[test]
    fn delete_tombstone() {
        let mut mt = make_memtable();
        mt.put(b"key1".to_vec(), 1, b"val1".to_vec());
        mt.delete(b"key1".to_vec(), 5);
        assert_eq!(mt.get(b"key1", 10), Some(None));
        assert_eq!(mt.get(b"key1", 3), Some(Some(b"val1".to_vec())));
    }

    #[test]
    fn range_scan() {
        let mut mt = make_memtable();
        mt.put(b"a".to_vec(), 1, b"1".to_vec());
        mt.put(b"b".to_vec(), 2, b"2".to_vec());
        mt.put(b"c".to_vec(), 3, b"3".to_vec());
        let results = mt.range_scan(b"a", b"d", 10);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(results[1], (b"b".to_vec(), b"2".to_vec()));
    }

    #[test]
    fn range_scan_skips_tombstones() {
        let mut mt = make_memtable();
        mt.put(b"a".to_vec(), 1, b"1".to_vec());
        mt.delete(b"b".to_vec(), 2);
        mt.put(b"c".to_vec(), 3, b"3".to_vec());
        let results = mt.range_scan(b"a", b"d", 10);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"a");
        assert_eq!(results[1].0, b"c");
    }

    #[test]
    fn freeze_preserves_data() {
        let mut mt = make_memtable();
        mt.put(b"k1".to_vec(), 1, b"v1".to_vec());
        mt.put(b"k2".to_vec(), 2, b"v2".to_vec());
        let frozen = mt.freeze();
        assert!(frozen.len() >= 2);
        assert_eq!(frozen.min_seqno, 1);
        assert_eq!(frozen.max_seqno, 2);
    }

    #[test]
    fn is_full() {
        let mut mt = MemTable::new(100);
        assert!(!mt.is_full());
        for i in 0..20u64 {
            let key = format!("key_{i:04}", i = i);
            let val = "x".repeat(10);
            mt.put(key.into_bytes(), i, val.into_bytes());
        }
        assert!(mt.is_full());
    }

    #[test]
    fn multiple_versions_coexist() {
        let mut mt = make_memtable();
        mt.put(b"k".to_vec(), 1, b"v1".to_vec());
        mt.put(b"k".to_vec(), 5, b"v5".to_vec());
        mt.put(b"k".to_vec(), 10, b"v10".to_vec());
        assert_eq!(mt.get(b"k", 10), Some(Some(b"v10".to_vec())));
        assert_eq!(mt.get(b"k", 7), Some(Some(b"v5".to_vec())));
        assert_eq!(mt.get(b"k", 3), Some(Some(b"v1".to_vec())));
        assert_eq!(mt.get(b"k", 0), None);
    }
}
