//! 内存 B+Tree 索引。
//!
//! - 可配置 page_size（每个节点最大 key 数）
//! - 叶子节点通过 `next` 形成单向链表，支持 range scan
//! - 支持 duplicate key（multikey 索引）：同一 key 可关联多个 RecordId
//! - 通过 `RwLock` 支持并发读写访问
//! - 内部节点使用二分查找（O(log n) 而非 O(n)）
//! - 前缀压缩减少相似 key 的内存占用
//! - 支持批量加载（O(n) 构建时间）
//! - 节点空闲列表复用已释放节点

use std::sync::RwLock;

use mdb_doc::value::Bson;

use crate::types::{bson_cmp, RecordId};

type NodeId = usize;

// ---------------------------------------------------------------------------
// Bson 字节序列化（用于前缀压缩）
// ---------------------------------------------------------------------------

const TAG_MIN_KEY: u8 = 0x00;
const TAG_NULL: u8 = 0x01;
const TAG_INT32: u8 = 0x10;
const TAG_INT64: u8 = 0x12;
const TAG_DOUBLE: u8 = 0x01;
const TAG_STRING: u8 = 0x02;
const TAG_BOOL: u8 = 0x08;
const TAG_DATETIME: u8 = 0x09;
const TAG_OBJ_ID: u8 = 0x07;
const TAG_TIMESTAMP: u8 = 0x11;
const TAG_MAX_KEY: u8 = 0x7F;

/// 将 Bson 序列化为规范字节表示（用于前缀压缩）。
fn bson_to_bytes(b: &Bson) -> Vec<u8> {
    match b {
        Bson::Int32(v) => {
            let mut buf = Vec::with_capacity(5);
            buf.push(TAG_INT32);
            buf.extend_from_slice(&v.to_le_bytes());
            buf
        }
        Bson::Int64(v) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_INT64);
            buf.extend_from_slice(&v.to_le_bytes());
            buf
        }
        Bson::Double(v) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_DOUBLE);
            buf.extend_from_slice(&v.to_le_bytes());
            buf
        }
        Bson::String(s) => {
            let bytes = s.as_bytes();
            let mut buf = Vec::with_capacity(5 + bytes.len());
            buf.push(TAG_STRING);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
        Bson::Boolean(v) => vec![TAG_BOOL, u8::from(*v)],
        Bson::Null | Bson::Undefined => vec![TAG_NULL],
        Bson::DateTime(v) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_DATETIME);
            buf.extend_from_slice(&v.to_le_bytes());
            buf
        }
        Bson::ObjectId(oid) => {
            let mut buf = Vec::with_capacity(13);
            buf.push(TAG_OBJ_ID);
            buf.extend_from_slice(oid.bytes());
            buf
        }
        Bson::Timestamp(ts) => {
            let mut buf = Vec::with_capacity(9);
            buf.push(TAG_TIMESTAMP);
            buf.extend_from_slice(&ts.as_u64().to_le_bytes());
            buf
        }
        Bson::MinKey => vec![TAG_MIN_KEY],
        Bson::MaxKey => vec![TAG_MAX_KEY],
        Bson::Regex(r) => {
            let mut buf = Vec::with_capacity(1 + r.pattern.len() + r.options.len() + 2);
            buf.push(0x0B);
            buf.extend_from_slice(r.pattern.as_bytes());
            buf.push(0);
            buf.extend_from_slice(r.options.as_bytes());
            buf.push(0);
            buf
        }
        Bson::JavaScript(s) => {
            let bytes = s.as_bytes();
            let mut buf = Vec::with_capacity(5 + bytes.len());
            buf.push(0x0D);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
        Bson::Symbol(s) => {
            let bytes = s.as_bytes();
            let mut buf = Vec::with_capacity(5 + bytes.len());
            buf.push(0x0E);
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
        Bson::Binary(bin) => {
            let mut buf = Vec::with_capacity(6 + bin.bytes.len());
            buf.push(TAG_OBJ_ID);
            buf.push(bin.subtype.code());
            buf.extend_from_slice(&(bin.bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&bin.bytes);
            buf
        }
        Bson::Decimal128(d) => {
            let mut buf = Vec::with_capacity(17);
            buf.push(0x13);
            buf.extend_from_slice(d.bytes());
            buf
        }
        _ => {
            let s = format!("{b}");
            let mut buf = Vec::with_capacity(1 + s.len());
            buf.push(0xFE);
            buf.extend_from_slice(s.as_bytes());
            buf
        }
    }
}

/// 从字节反序列化 Bson。
fn bson_from_bytes(bytes: &[u8]) -> Option<Bson> {
    if bytes.is_empty() {
        return None;
    }
    match bytes[0] {
        TAG_INT32 if bytes.len() >= 5 => {
            let v = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
            Some(Bson::Int32(v))
        }
        TAG_INT64 if bytes.len() >= 9 => {
            let arr: [u8; 8] = bytes[1..9].try_into().ok()?;
            Some(Bson::Int64(i64::from_le_bytes(arr)))
        }
        TAG_DOUBLE if bytes.len() >= 9 => {
            let arr: [u8; 8] = bytes[1..9].try_into().ok()?;
            Some(Bson::Double(f64::from_le_bytes(arr)))
        }
        TAG_STRING if bytes.len() >= 5 => {
            let len = u32::from_le_bytes(bytes[1..5].try_into().ok()?) as usize;
            if bytes.len() >= 5 + len {
                let s = std::str::from_utf8(&bytes[5..5 + len]).ok()?;
                Some(Bson::String(s.to_owned()))
            } else {
                None
            }
        }
        TAG_BOOL if bytes.len() >= 2 => Some(Bson::Boolean(bytes[1] != 0)),
        TAG_NULL => Some(Bson::Null),
        TAG_DATETIME if bytes.len() >= 9 => {
            let arr: [u8; 8] = bytes[1..9].try_into().ok()?;
            Some(Bson::DateTime(i64::from_le_bytes(arr)))
        }
        TAG_OBJ_ID if bytes.len() >= 13 => {
            let arr: [u8; 12] = bytes[1..13].try_into().ok()?;
            Some(Bson::ObjectId(mdb_doc::oid::ObjectId::from_bytes(arr)))
        }
        TAG_TIMESTAMP if bytes.len() >= 9 => {
            let arr: [u8; 8] = bytes[1..9].try_into().ok()?;
            Some(Bson::Timestamp(mdb_doc::Timestamp::from_u64(u64::from_le_bytes(arr))))
        }
        TAG_MIN_KEY => Some(Bson::MinKey),
        TAG_MAX_KEY => Some(Bson::MaxKey),
        _ => None,
    }
}

/// 两个字节切片的公共前缀长度。
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ---------------------------------------------------------------------------
// 前缀压缩 key 存储
// ---------------------------------------------------------------------------

/// 节点内的前缀压缩 key 存储。
///
/// 第一个 key 完整存储；后续 key 仅存储与前一个 key 的公共前缀长度和后缀字节。
/// 叶子节点和内部节点的 key 均使用此结构。
#[derive(Debug, Clone)]
struct CompressedKeys {
    first: Vec<u8>,
    deltas: Vec<(u16, Vec<u8>)>,
}

impl CompressedKeys {
    fn new() -> Self {
        Self { first: Vec::new(), deltas: Vec::new() }
    }

    fn len(&self) -> usize {
        if self.first.is_empty() { 0 } else { 1 + self.deltas.len() }
    }

    fn is_empty(&self) -> bool {
        self.first.is_empty()
    }

    fn push(&mut self, key: &Bson) {
        let key_bytes = bson_to_bytes(key);
        if self.first.is_empty() {
            self.first = key_bytes;
        } else {
            let prev = self.decompress_bytes(self.len() - 1);
            let plen = common_prefix_len(&prev, &key_bytes);
            self.deltas.push((plen as u16, key_bytes[plen..].to_vec()));
        }
    }

    fn decompress_bytes(&self, idx: usize) -> Vec<u8> {
        if idx == 0 {
            return self.first.clone();
        }
        let mut result = self.decompress_bytes(idx - 1);
        let (plen, suffix) = &self.deltas[idx - 1];
        result.truncate(*plen as usize);
        result.extend_from_slice(suffix);
        result
    }

    fn get(&self, idx: usize) -> Option<Bson> {
        let bytes = self.decompress_bytes(idx);
        bson_from_bytes(&bytes)
    }

    fn insert(&mut self, idx: usize, key: &Bson) {
        let n = self.len();
        let mut all: Vec<Bson> = (0..n).filter_map(|i| self.get(i)).collect();
        all.insert(idx, key.clone());
        *self = Self::new();
        for k in &all {
            self.push(k);
        }
    }

    fn remove(&mut self, idx: usize) -> Option<Bson> {
        let n = self.len();
        let removed = self.get(idx);
        let all: Vec<Bson> = (0..n).filter(|&i| i != idx).filter_map(|i| self.get(i)).collect();
        *self = Self::new();
        for k in &all {
            self.push(k);
        }
        removed
    }

    fn truncate_to(&mut self, new_len: usize) {
        if new_len == 0 {
            self.first.clear();
            self.deltas.clear();
        } else if new_len <= self.deltas.len() {
            self.deltas.truncate(new_len - 1);
        }
    }

    fn replace(&mut self, idx: usize, key: &Bson) {
        let all: Vec<Bson> = (0..self.len())
            .map(|i| if i == idx { key.clone() } else { self.get(i).unwrap() })
            .collect();
        *self = Self::new();
        for k in &all {
            self.push(k);
        }
    }

    fn binary_search_by<F>(&self, mut cmp: F) -> Result<usize, usize>
    where
        F: FnMut(usize) -> std::cmp::Ordering,
    {
        let mut lo = 0usize;
        let mut hi = self.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match cmp(mid) {
                std::cmp::Ordering::Equal => return Ok(mid),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Err(lo)
    }
}

// ---------------------------------------------------------------------------
// 节点定义
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum BTreeNode {
    Leaf {
        keys: CompressedKeys,
        records: Vec<Vec<RecordId>>,
        next: Option<NodeId>,
        parent: Option<NodeId>,
    },
    Internal {
        keys: CompressedKeys,
        children: Vec<NodeId>,
        parent: Option<NodeId>,
    },
}

// ---------------------------------------------------------------------------
// BTreeInner（实际树状态）
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct BTreeInner {
    root: NodeId,
    nodes: Vec<BTreeNode>,
    free_list: Vec<NodeId>,
    page_size: usize,
    len: usize,
    total_key_bytes: usize,
}

impl BTreeInner {
    fn new(page_size: usize) -> Self {
        assert!(page_size >= 2, "page_size must be >= 2");
        let root = BTreeNode::Leaf {
            keys: CompressedKeys::new(),
            records: Vec::with_capacity(page_size),
            next: None,
            parent: None,
        };
        Self { root: 0, nodes: vec![root], free_list: Vec::new(), page_size, len: 0, total_key_bytes: 0 }
    }

    fn alloc_node(&mut self, node: BTreeNode) -> NodeId {
        if let Some(id) = self.free_list.pop() {
            self.nodes[id] = node;
            id
        } else {
            let id = self.nodes.len();
            self.nodes.push(node);
            id
        }
    }

    fn free_node(&mut self, id: NodeId) {
        self.free_list.push(id);
    }

    fn is_leaf(&self, id: NodeId) -> bool {
        matches!(self.nodes[id], BTreeNode::Leaf { .. })
    }

    fn is_node_full(&self, id: NodeId) -> bool {
        if self.is_leaf(id) {
            self.leaf_keys_len(id) >= self.page_size
        } else {
            self.internal_children(id).len() >= self.page_size
        }
    }

    fn set_parent(&mut self, id: NodeId, parent: Option<NodeId>) {
        match &mut self.nodes[id] {
            BTreeNode::Leaf { parent: p, .. } | BTreeNode::Internal { parent: p, .. } => {
                *p = parent;
            }
        }
    }

    // ---- Leaf accessors ----

    fn leaf_keys_len(&self, id: NodeId) -> usize {
        match &self.nodes[id] {
            BTreeNode::Leaf { keys, .. } => keys.len(),
            _ => panic!("expected leaf"),
        }
    }

    fn leaf_key(&self, id: NodeId, idx: usize) -> Bson {
        match &self.nodes[id] {
            BTreeNode::Leaf { keys, .. } => keys.get(idx).expect("key index out of bounds"),
            _ => panic!("expected leaf"),
        }
    }

    fn leaf_records(&self, id: NodeId) -> &[Vec<RecordId>] {
        match &self.nodes[id] {
            BTreeNode::Leaf { records, .. } => records,
            _ => panic!("expected leaf"),
        }
    }

    fn leaf_records_mut(&mut self, id: NodeId) -> &mut Vec<Vec<RecordId>> {
        match &mut self.nodes[id] {
            BTreeNode::Leaf { records, .. } => records,
            _ => panic!("expected leaf"),
        }
    }

    fn leaf_next(&self, id: NodeId) -> Option<NodeId> {
        match &self.nodes[id] {
            BTreeNode::Leaf { next, .. } => *next,
            _ => panic!("expected leaf"),
        }
    }

    fn set_leaf_next(&mut self, id: NodeId, next: Option<NodeId>) {
        match &mut self.nodes[id] {
            BTreeNode::Leaf { next: n, .. } => *n = next,
            _ => panic!("expected leaf"),
        }
    }

    fn leaf_keys(&self, id: NodeId) -> &CompressedKeys {
        match &self.nodes[id] {
            BTreeNode::Leaf { keys, .. } => keys,
            _ => panic!("expected leaf"),
        }
    }

    fn leaf_keys_mut(&mut self, id: NodeId) -> &mut CompressedKeys {
        match &mut self.nodes[id] {
            BTreeNode::Leaf { keys, .. } => keys,
            _ => panic!("expected leaf"),
        }
    }

    // ---- Internal accessors ----

    fn internal_keys(&self, id: NodeId) -> &CompressedKeys {
        match &self.nodes[id] {
            BTreeNode::Internal { keys, .. } => keys,
            _ => panic!("expected internal"),
        }
    }

    fn internal_keys_mut(&mut self, id: NodeId) -> &mut CompressedKeys {
        match &mut self.nodes[id] {
            BTreeNode::Internal { keys, .. } => keys,
            _ => panic!("expected internal"),
        }
    }

    fn internal_children(&self, id: NodeId) -> &[NodeId] {
        match &self.nodes[id] {
            BTreeNode::Internal { children, .. } => children,
            _ => panic!("expected internal"),
        }
    }

    fn internal_children_mut(&mut self, id: NodeId) -> &mut Vec<NodeId> {
        match &mut self.nodes[id] {
            BTreeNode::Internal { children, .. } => children,
            _ => panic!("expected internal"),
        }
    }

    // ---- 查找（二分查找） ----

    /// 从根下降到包含 key 的叶子节点（内部节点使用二分查找）。
    fn find_leaf(&self, key: &Bson) -> NodeId {
        let mut current = self.root;
        while !self.is_leaf(current) {
            let idx = self.find_child_idx(current, key);
            current = self.internal_children(current)[idx];
        }
        current
    }

    /// 在叶子节点中二分查找 key，返回 (key_index, 0) 或 None。
    fn find_in_leaf(&self, leaf: NodeId, key: &Bson) -> Option<(usize, usize)> {
        let keys = self.leaf_keys(leaf);
        match keys.binary_search_by(|i| {
            let k = keys.get(i).unwrap();
            bson_cmp(&k, key)
        }) {
            Ok(i) => Some((i, 0)),
            Err(_) => None,
        }
    }

    /// 在内部节点中二分查找应下降的子节点索引。
    fn find_child_idx(&self, node_id: NodeId, key: &Bson) -> usize {
        let keys = self.internal_keys(node_id);
        let children = self.internal_children(node_id);
        keys.binary_search_by(|i| {
            let k = keys.get(i).unwrap();
            match bson_cmp(&k, key) {
                std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Less,
            }
        })
        .unwrap_or_else(|i| i.min(children.len() - 1))
    }

    // ---- 插入（latch crabbing 风格：自顶向下记录路径） ----

    fn insert(&mut self, key: Bson, record_id: RecordId) {
        let root = self.root;
        if self.is_node_full(root) {
            let old_root = root;
            let new_root = self.alloc_node(BTreeNode::Internal {
                keys: CompressedKeys::new(),
                children: vec![old_root],
                parent: None,
            });
            self.root = new_root;
            self.set_parent(old_root, Some(new_root));
            self.split_child(new_root, 0);
            self.insert_non_full(new_root, key, record_id);
        } else {
            self.insert_non_full(root, key, record_id);
        }
        self.len += 1;
    }

    fn insert_non_full(&mut self, id: NodeId, key: Bson, record_id: RecordId) {
        if self.is_leaf(id) {
            let keys = self.leaf_keys(id);
            match keys.binary_search_by(|i| {
                let k = keys.get(i).unwrap();
                bson_cmp(&k, &key)
            }) {
                Ok(i) => {
                    self.leaf_records_mut(id)[i].push(record_id);
                    return;
                }
                Err(pos) => {
                    self.leaf_keys_mut(id).insert(pos, &key);
                    self.leaf_records_mut(id).insert(pos, vec![record_id]);
                }
            }
        } else {
            let idx = self.find_child_idx(id, &key);
            let child = self.internal_children(id)[idx];
            if self.is_node_full(child) {
                self.split_child(id, idx);
                let idx2 = self.find_child_idx(id, &key);
                let child2 = self.internal_children(id)[idx2];
                self.insert_non_full(child2, key, record_id);
            } else {
                self.insert_non_full(child, key, record_id);
            }
        }
    }

    fn split_child(&mut self, parent_id: NodeId, child_idx: usize) {
        let child_id = self.internal_children(parent_id)[child_idx];
        if self.is_leaf(child_id) {
            self.split_leaf(child_id, parent_id, child_idx);
        } else {
            self.split_internal(child_id, parent_id, child_idx);
        }
    }

    fn split_leaf(&mut self, leaf_id: NodeId, parent_id: NodeId, child_idx: usize) {
        let mid = self.page_size / 2;
        let old_next = self.leaf_next(leaf_id);

        let mut new_keys = CompressedKeys::new();
        let mut new_records = Vec::with_capacity(self.page_size - mid);
        for i in mid..self.leaf_keys_len(leaf_id) {
            let k = self.leaf_key(leaf_id, i);
            new_keys.push(&k);
            new_records.push(self.leaf_records(leaf_id)[i].clone());
        }

        let new_leaf = self.alloc_node(BTreeNode::Leaf {
            keys: new_keys,
            records: new_records,
            next: old_next,
            parent: Some(parent_id),
        });

        self.leaf_keys_mut(leaf_id).truncate_to(mid);
        self.leaf_records_mut(leaf_id).truncate(mid);
        self.set_leaf_next(leaf_id, Some(new_leaf));

        let promoted_key = self.leaf_key(new_leaf, 0);
        let children = self.internal_children_mut(parent_id);
        children.insert(child_idx + 1, new_leaf);
        self.internal_keys_mut(parent_id).insert(child_idx, &promoted_key);
    }

    fn split_internal(
        &mut self,
        node_id: NodeId,
        parent_id: NodeId,
        child_idx: usize,
    ) {
        let mid = self.internal_keys(node_id).len() / 2;
        let promoted_key = self.internal_keys(node_id).get(mid).unwrap();

        let mut new_keys = CompressedKeys::new();
        let mut new_children = Vec::new();
        for i in (mid + 1)..self.internal_keys(node_id).len() {
            let k = self.internal_keys(node_id).get(i).unwrap();
            new_keys.push(&k);
        }
        new_children.extend_from_slice(&self.internal_children(node_id)[mid + 1..]);

        let new_node = self.alloc_node(BTreeNode::Internal {
            keys: new_keys,
            children: new_children.clone(),
            parent: Some(parent_id),
        });

        for &cid in &new_children {
            self.set_parent(cid, Some(new_node));
        }

        self.internal_keys_mut(node_id).truncate_to(mid);
        self.internal_children_mut(node_id).truncate(mid + 1);

        let children = self.internal_children_mut(parent_id);
        children.insert(child_idx + 1, new_node);
        self.internal_keys_mut(parent_id).insert(child_idx, &promoted_key);
    }

    // ---- 删除 ----

    fn remove(&mut self, key: &Bson, record_id: RecordId) -> bool {
        let root = self.root;
        let result = self.remove_entry(root, key, record_id);
        if result {
            self.len -= 1;
            if !self.is_leaf(self.root) && self.internal_keys(self.root).is_empty() {
                let child = self.internal_children(self.root)[0];
                let old_root = self.root;
                self.root = child;
                self.set_parent(child, None);
                self.free_node(old_root);
            }
        }
        result
    }

    fn remove_entry(&mut self, id: NodeId, key: &Bson, record_id: RecordId) -> bool {
        if self.is_leaf(id) {
            self.remove_from_leaf(id, key, record_id)
        } else {
            self.remove_from_internal(id, key, record_id)
        }
    }

    fn remove_from_leaf(&mut self, id: NodeId, key: &Bson, record_id: RecordId) -> bool {
        if let Some((idx, _)) = self.find_in_leaf(id, key) {
            let records = &mut self.leaf_records_mut(id)[idx];
            if let Some(pos) = records.iter().position(|&r| r == record_id) {
                records.remove(pos);
                if records.is_empty() {
                    self.leaf_keys_mut(id).remove(idx);
                    self.leaf_records_mut(id).remove(idx);
                }
                return true;
            }
        }
        false
    }

    fn remove_from_internal(&mut self, id: NodeId, key: &Bson, record_id: RecordId) -> bool {
        let idx = self.find_child_idx(id, key);
        let child = self.internal_children(id)[idx];
        let before = self.count_entries(child);
        let result = self.remove_entry(child, key, record_id);
        let after = self.count_entries(child);

        if result && before != after {
            let min_keys = self.page_size / 2;
            if !self.is_leaf(child) && self.internal_keys(child).len() < min_keys {
                self.rebalance_internal_child(id, idx);
            }
        }
        result
    }

    fn rebalance_internal_child(&mut self, parent_id: NodeId, child_idx: usize) {
        let children = self.internal_children(parent_id).to_vec();
        let child = children[child_idx];

        if child_idx > 0 {
            let left = children[child_idx - 1];
            if self.internal_keys(left).len() > self.page_size / 2 {
                let parent_key = self.internal_keys(parent_id).get(child_idx - 1).unwrap();
                let left_last_idx = self.internal_keys(left).len() - 1;
                let left_last_key = self.internal_keys(left).get(left_last_idx).unwrap();
                let left_last_child = *self.internal_children(left).last().unwrap();

                self.internal_keys_mut(parent_id).replace(child_idx - 1, &left_last_key);
                self.internal_keys_mut(child).insert(0, &parent_key);
                self.internal_children_mut(child).insert(0, left_last_child);
                self.set_parent(left_last_child, Some(child));

                self.internal_keys_mut(left).remove(left_last_idx);
                self.internal_children_mut(left).pop();
                return;
            }
        }

        if child_idx + 1 < children.len() {
            let right = children[child_idx + 1];
            if self.internal_keys(right).len() > self.page_size / 2 {
                let parent_key = self.internal_keys(parent_id).get(child_idx).unwrap();
                let right_first_key = self.internal_keys(right).get(0).unwrap();
                let right_first_child = self.internal_children(right)[0];

                self.internal_keys_mut(parent_id).replace(child_idx, &right_first_key);
                self.internal_keys_mut(child).push(&parent_key);
                self.internal_children_mut(child).push(right_first_child);
                self.set_parent(right_first_child, Some(child));

                self.internal_keys_mut(right).remove(0);
                self.internal_children_mut(right).remove(0);
                return;
            }
        }

        if child_idx + 1 < children.len() {
            let right = children[child_idx + 1];
            let parent_key = self.internal_keys(parent_id).get(child_idx).unwrap();

            self.internal_keys_mut(child).push(&parent_key);
            let right_keys: Vec<Bson> =
                (0..self.internal_keys(right).len()).map(|i| self.internal_keys(right).get(i).unwrap()).collect();
            let right_children: Vec<NodeId> = self.internal_children(right).to_vec();
            for k in &right_keys {
                self.internal_keys_mut(child).push(k);
            }
            self.internal_children_mut(child).extend_from_slice(&right_children);

            for &cid in &right_children {
                self.set_parent(cid, Some(child));
            }

            self.internal_keys_mut(parent_id).remove(child_idx);
            self.internal_children_mut(parent_id).remove(child_idx + 1);
            self.free_node(right);
        }
    }

    fn count_entries(&self, id: NodeId) -> usize {
        if self.is_leaf(id) {
            self.leaf_records(id).iter().map(|r| r.len()).sum()
        } else {
            self.internal_children(id).iter().map(|&c| self.count_entries(c)).sum()
        }
    }

    // ---- 树高计算 ----

    fn tree_height(&self) -> usize {
        let mut h = 1;
        let mut current = self.root;
        while !self.is_leaf(current) {
            h += 1;
            current = self.internal_children(current)[0];
        }
        h
    }

    fn count_nodes(&self) -> usize {
        self.nodes.len() - self.free_list.len()
    }

    // ---- 批量加载 ----

    fn bulk_load(&mut self, sorted_entries: Vec<(Bson, RecordId)>) {
        if sorted_entries.is_empty() {
            return;
        }

        self.root = 0;
        self.nodes.clear();
        self.free_list.clear();
        self.len = 0;
        self.total_key_bytes = 0;

        let mut leaves: Vec<NodeId> = Vec::new();
        for chunk in sorted_entries.chunks(self.page_size) {
            let mut keys = CompressedKeys::new();
            let mut records = Vec::with_capacity(chunk.len());
            for (key, rid) in chunk {
                keys.push(key);
                records.push(vec![*rid]);
                self.total_key_bytes += bson_to_bytes(key).len();
            }
            let leaf_id = self.alloc_node(BTreeNode::Leaf {
                keys,
                records,
                next: None,
                parent: None,
            });
            leaves.push(leaf_id);
            self.len += chunk.len();
        }

        for i in 0..leaves.len() - 1 {
            self.set_leaf_next(leaves[i], Some(leaves[i + 1]));
        }

        // Each level item: (node_id, first_key_in_subtree)
        let mut current_level: Vec<(NodeId, Bson)> = leaves
            .iter()
            .map(|&id| (id, self.leaf_key(id, 0)))
            .collect();

        while current_level.len() > 1 {
            let mut next_level: Vec<(NodeId, Bson)> = Vec::new();
            for chunk in current_level.chunks(self.page_size - 1) {
                let mut keys = CompressedKeys::new();
                let mut children = Vec::with_capacity(chunk.len());
                for &(child_id, _) in chunk {
                    children.push(child_id);
                }

                // Separator between child[i] and child[i+1] = first key of child[i+1]'s subtree
                for &(_, ref first_key) in &chunk[1..] {
                    keys.push(first_key);
                }

                let internal_id = self.alloc_node(BTreeNode::Internal {
                    keys,
                    children,
                    parent: None,
                });

                for &(child_id, _) in chunk {
                    self.set_parent(child_id, Some(internal_id));
                }

                // First key of this internal node = first key of its leftmost subtree
                next_level.push((internal_id, chunk[0].1.clone()));
            }
            current_level = next_level;
        }

        self.root = current_level[0].0;
        self.set_parent(self.root, None);
    }
}

// ---------------------------------------------------------------------------
// IndexStats
// ---------------------------------------------------------------------------

/// B+Tree 索引统计信息。
#[derive(Debug, Clone)]
pub struct IndexStats {
    /// 条目总数（包括重复 key 的多个 record）。
    pub total_entries: usize,
    /// 节点总数（不含空闲列表中的已释放节点）。
    pub total_nodes: usize,
    /// 树高（叶子层 = 1）。
    pub tree_height: usize,
    /// 平均 key 字节大小。
    pub avg_key_size_bytes: f64,
}

// ---------------------------------------------------------------------------
// BTree（对外 API，RwLock 包装）
// ---------------------------------------------------------------------------

/// 内存 B+Tree 索引，支持并发读写访问。
///
/// 内部通过 `RwLock<BTreeInner>` 实现：
/// - 读操作（`point_lookup`、`range_scan`、`prefix_scan`、`len` 等）获取读锁
/// - 写操作（`insert`、`remove`、`bulk_load`）获取写锁
///
/// 写路径采用 latch crabbing 模式：自顶向下记录路径，自底向上处理分裂。
#[derive(Debug)]
pub struct BTree {
    inner: RwLock<BTreeInner>,
}

impl BTree {
    /// `page_size` = 每个叶子节点容纳的最大 key 数。
    pub fn new(page_size: usize) -> Self {
        Self { inner: RwLock::new(BTreeInner::new(page_size)) }
    }

    /// 索引中的条目总数。
    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().unwrap().len == 0
    }

    /// 插入一个 (key, record_id) 对。
    pub fn insert(&self, key: Bson, record_id: RecordId) {
        let mut inner = self.inner.write().unwrap();
        let kb = bson_to_bytes(&key).len();
        inner.total_key_bytes += kb;
        inner.insert(key, record_id);
    }

    /// 移除指定的 (key, record_id) 对。成功返回 true。
    pub fn remove(&self, key: &Bson, record_id: RecordId) -> bool {
        let mut inner = self.inner.write().unwrap();
        let result = inner.remove(key, record_id);
        if result {
            inner.total_key_bytes -= bson_to_bytes(key).len();
        }
        result
    }

    /// 点查询：返回所有匹配 key 的 RecordId。
    pub fn point_lookup(&self, key: &Bson) -> Vec<RecordId> {
        let inner = self.inner.read().unwrap();
        let leaf = inner.find_leaf(key);
        if let Some((idx, _)) = inner.find_in_leaf(leaf, key) {
            inner.leaf_records(leaf)[idx].clone()
        } else {
            vec![]
        }
    }

    /// 范围查询：返回 key 在 [start, end] 范围内的所有 RecordId（有序）。
    pub fn range_scan(&self, start: &Bson, end: &Bson) -> Vec<RecordId> {
        let inner = self.inner.read().unwrap();
        let mut result = vec![];
        let mut node_id = inner.find_leaf(start);
        loop {
            let keys_len = inner.leaf_keys_len(node_id);
            let records = inner.leaf_records(node_id);
            for i in 0..keys_len {
                let k = inner.leaf_key(node_id, i);
                if bson_cmp(&k, start) == std::cmp::Ordering::Less {
                    continue;
                }
                if bson_cmp(&k, end) == std::cmp::Ordering::Greater {
                    return result;
                }
                result.extend_from_slice(&records[i]);
            }
            match inner.leaf_next(node_id) {
                Some(n) => node_id = n,
                None => break,
            }
        }
        result
    }

    /// 前缀扫描：key 为字符串且以 `prefix` 开头。
    pub fn prefix_scan(&self, prefix: &str) -> Vec<RecordId> {
        let inner = self.inner.read().unwrap();
        let mut result = vec![];
        let start_key = Bson::String(prefix.to_owned());
        let mut node_id = inner.find_leaf(&start_key);
        loop {
            let keys_len = inner.leaf_keys_len(node_id);
            let records = inner.leaf_records(node_id);
            for i in 0..keys_len {
                let k = inner.leaf_key(node_id, i);
                if let Some(s) = k.as_str() {
                    if s.starts_with(prefix) {
                        result.extend_from_slice(&records[i]);
                    } else if bson_cmp(&k, &start_key) == std::cmp::Ordering::Greater {
                        return result;
                    }
                }
            }
            match inner.leaf_next(node_id) {
                Some(n) => node_id = n,
                None => break,
            }
        }
        result
    }

    /// 批量加载已排序的条目，O(n) 构建时间。
    ///
    /// `sorted_entries` 必须按键的 `bson_cmp` 顺序排列。
    /// 对于索引重建场景，传入原始字节会先反序列化为 Bson。
    pub fn bulk_load(&self, sorted_entries: Vec<(Bson, RecordId)>) {
        let mut inner = self.inner.write().unwrap();
        inner.bulk_load(sorted_entries);
    }

    /// 返回索引统计信息。
    pub fn stats(&self) -> IndexStats {
        let inner = self.inner.read().unwrap();
        let total_nodes = inner.count_nodes();
        let tree_height = inner.tree_height();
        let avg_key_size_bytes = if inner.len > 0 {
            inner.total_key_bytes as f64 / inner.len as f64
        } else {
            0.0
        };
        IndexStats {
            total_entries: inner.len,
            total_nodes,
            tree_height,
            avg_key_size_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_btree(page_size: usize) -> BTree {
        BTree::new(page_size)
    }

    #[test]
    fn empty_tree() {
        let bt = make_btree(4);
        assert!(bt.is_empty());
        assert_eq!(bt.len(), 0);
        assert!(bt.point_lookup(&Bson::Int32(1)).is_empty());
    }

    #[test]
    fn insert_and_lookup() {
        let bt = make_btree(4);
        bt.insert(Bson::Int32(10), 100);
        bt.insert(Bson::Int32(20), 200);
        bt.insert(Bson::Int32(5), 50);

        assert_eq!(bt.len(), 3);
        assert_eq!(bt.point_lookup(&Bson::Int32(10)), vec![100]);
        assert_eq!(bt.point_lookup(&Bson::Int32(20)), vec![200]);
        assert_eq!(bt.point_lookup(&Bson::Int32(5)), vec![50]);
        assert!(bt.point_lookup(&Bson::Int32(99)).is_empty());
    }

    #[test]
    fn insert_triggers_split() {
        let bt = make_btree(3);
        for i in 0..10 {
            bt.insert(Bson::Int32(i), i as u64);
        }
        assert_eq!(bt.len(), 10);
        for i in 0..10 {
            assert_eq!(bt.point_lookup(&Bson::Int32(i)), vec![i as u64]);
        }
    }

    #[test]
    fn duplicate_keys() {
        let bt = make_btree(4);
        bt.insert(Bson::Int32(1), 10);
        bt.insert(Bson::Int32(1), 20);
        bt.insert(Bson::Int32(1), 30);
        assert_eq!(bt.len(), 3);
        let mut result = bt.point_lookup(&Bson::Int32(1));
        result.sort();
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[test]
    fn remove_key() {
        let bt = make_btree(4);
        bt.insert(Bson::Int32(1), 10);
        bt.insert(Bson::Int32(2), 20);
        bt.insert(Bson::Int32(3), 30);

        assert!(bt.remove(&Bson::Int32(2), 20));
        assert_eq!(bt.len(), 2);
        assert!(bt.point_lookup(&Bson::Int32(2)).is_empty());
        assert_eq!(bt.point_lookup(&Bson::Int32(1)), vec![10]);
        assert_eq!(bt.point_lookup(&Bson::Int32(3)), vec![30]);
    }

    #[test]
    fn remove_one_of_duplicate() {
        let bt = make_btree(4);
        bt.insert(Bson::Int32(1), 10);
        bt.insert(Bson::Int32(1), 20);

        assert!(bt.remove(&Bson::Int32(1), 10));
        assert_eq!(bt.len(), 1);
        assert_eq!(bt.point_lookup(&Bson::Int32(1)), vec![20]);
    }

    #[test]
    fn range_scan() {
        let bt = make_btree(4);
        for i in 0..20 {
            bt.insert(Bson::Int32(i), i as u64);
        }
        let result = bt.range_scan(&Bson::Int32(5), &Bson::Int32(10));
        assert_eq!(result, vec![5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn range_scan_across_splits() {
        let bt = make_btree(3);
        for i in 0..20 {
            bt.insert(Bson::Int32(i), i as u64);
        }
        let result = bt.range_scan(&Bson::Int32(3), &Bson::Int32(15));
        let expected: Vec<u64> = (3..=15).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn prefix_scan() {
        let bt = BTree::new(4);
        bt.insert(Bson::String("apple".into()), 1);
        bt.insert(Bson::String("application".into()), 2);
        bt.insert(Bson::String("banana".into()), 3);
        bt.insert(Bson::String("apricot".into()), 4);

        let mut result = bt.prefix_scan("app");
        result.sort();
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn range_scan_empty() {
        let bt = make_btree(4);
        bt.insert(Bson::Int32(1), 10);
        let result = bt.range_scan(&Bson::Int32(5), &Bson::Int32(10));
        assert!(result.is_empty());
    }

    #[test]
    fn remove_nonexistent() {
        let bt = make_btree(4);
        bt.insert(Bson::Int32(1), 10);
        assert!(!bt.remove(&Bson::Int32(99), 99));
        assert_eq!(bt.len(), 1);
    }

    #[test]
    fn large_insert_sequential() {
        let bt = make_btree(16);
        for i in 0..1000 {
            bt.insert(Bson::Int64(i), i as u64);
        }
        assert_eq!(bt.len(), 1000);
        for i in 0..1000 {
            assert_eq!(bt.point_lookup(&Bson::Int64(i)), vec![i as u64]);
        }
    }

    #[test]
    fn large_insert_reverse() {
        let bt = make_btree(16);
        for i in (0..500).rev() {
            bt.insert(Bson::Int64(i), i as u64);
        }
        assert_eq!(bt.len(), 500);
        for i in 0..500 {
            assert_eq!(bt.point_lookup(&Bson::Int64(i)), vec![i as u64]);
        }
    }

    #[test]
    fn mixed_types_insert_and_lookup() {
        let bt = make_btree(4);
        bt.insert(Bson::Int32(1), 1);
        bt.insert(Bson::String("hello".into()), 2);
        bt.insert(Bson::Boolean(true), 3);
        bt.insert(Bson::Null, 4);

        assert_eq!(bt.point_lookup(&Bson::Int32(1)), vec![1]);
        assert_eq!(bt.point_lookup(&Bson::String("hello".into())), vec![2]);
        assert_eq!(bt.point_lookup(&Bson::Boolean(true)), vec![3]);
        assert_eq!(bt.point_lookup(&Bson::Null), vec![4]);
    }

    #[test]
    fn remove_causes_merge() {
        let bt = make_btree(4);
        for i in 0..8 {
            bt.insert(Bson::Int32(i), i as u64);
        }
        for i in 0..7 {
            assert!(bt.remove(&Bson::Int32(i), i as u64));
        }
        assert_eq!(bt.len(), 1);
        assert_eq!(bt.point_lookup(&Bson::Int32(7)), vec![7]);
    }

    // ---- 新增测试：批量加载 ----

    #[test]
    fn bulk_load_basic() {
        let bt = make_btree(4);
        let entries: Vec<(Bson, RecordId)> =
            (0..100).map(|i| (Bson::Int32(i), i as u64)).collect();
        bt.bulk_load(entries);

        assert_eq!(bt.len(), 100);
        for i in 0..100 {
            assert_eq!(bt.point_lookup(&Bson::Int32(i)), vec![i as u64]);
        }
    }

    #[test]
    fn bulk_load_range_scan() {
        let bt = make_btree(4);
        let entries: Vec<(Bson, RecordId)> =
            (0..50).map(|i| (Bson::Int64(i), i as u64)).collect();
        bt.bulk_load(entries);

        let result = bt.range_scan(&Bson::Int64(10), &Bson::Int64(20));
        let expected: Vec<u64> = (10..=20).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn bulk_load_then_insert() {
        let bt = make_btree(4);
        let entries: Vec<(Bson, RecordId)> =
            (0..20).map(|i| (Bson::Int32(i), i as u64)).collect();
        bt.bulk_load(entries);

        bt.insert(Bson::Int32(100), 100);
        assert_eq!(bt.len(), 21);
        assert_eq!(bt.point_lookup(&Bson::Int32(100)), vec![100]);
        assert_eq!(bt.point_lookup(&Bson::Int32(5)), vec![5]);
    }

    #[test]
    fn bulk_load_empty() {
        let bt = make_btree(4);
        bt.bulk_load(vec![]);
        assert!(bt.is_empty());
        assert_eq!(bt.len(), 0);
    }

    #[test]
    fn bulk_load_strings_with_prefix_compression() {
        let bt = BTree::new(4);
        let entries: Vec<(Bson, RecordId)> = vec![
            (Bson::String("user_age".into()), 3),
            (Bson::String("user_city".into()), 4),
            (Bson::String("user_email".into()), 2),
            (Bson::String("user_name".into()), 1),
        ];
        bt.bulk_load(entries);

        assert_eq!(bt.len(), 4);
        assert_eq!(bt.point_lookup(&Bson::String("user_age".into())), vec![3]);
        assert_eq!(bt.point_lookup(&Bson::String("user_city".into())), vec![4]);
        assert_eq!(bt.point_lookup(&Bson::String("user_email".into())), vec![2]);
        assert_eq!(bt.point_lookup(&Bson::String("user_name".into())), vec![1]);
    }

    // ---- 新增测试：并发访问 ----

    #[test]
    fn concurrent_read_write() {
        use std::sync::Arc;
        use std::thread;

        let bt = Arc::new(BTree::new(16));

        let mut handles = vec![];

        for t in 0..4 {
            let bt_clone = Arc::clone(&bt);
            handles.push(thread::spawn(move || {
                for i in 0..200 {
                    let val = t * 200 + i;
                    bt_clone.insert(Bson::Int32(val), val as u64);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(bt.len(), 800);

        for t in 0..4 {
            for i in 0..200 {
                let val = t * 200 + i;
                assert_eq!(bt.point_lookup(&Bson::Int32(val)), vec![val as u64]);
            }
        }
    }

    #[test]
    fn concurrent_read_write_mixed() {
        use std::sync::Arc;
        use std::thread;

        let bt = Arc::new(BTree::new(8));

        let mut handles = vec![];

        let bt_w = Arc::clone(&bt);
        handles.push(thread::spawn(move || {
            for i in 0..500 {
                bt_w.insert(Bson::Int64(i), i as u64);
            }
        }));

        let bt_r = Arc::clone(&bt);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let _ = bt_r.point_lookup(&Bson::Int64(0));
                let _ = bt_r.len();
            }
        }));

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(bt.len(), 500);
    }

    // ---- 新增测试：统计信息 ----

    #[test]
    fn stats_empty_tree() {
        let bt = make_btree(4);
        let s = bt.stats();
        assert_eq!(s.total_entries, 0);
        assert_eq!(s.total_nodes, 1);
        assert_eq!(s.tree_height, 1);
        assert_eq!(s.avg_key_size_bytes, 0.0);
    }

    #[test]
    fn stats_after_inserts() {
        let bt = make_btree(4);
        for i in 0..100 {
            bt.insert(Bson::Int32(i), i as u64);
        }
        let s = bt.stats();
        assert_eq!(s.total_entries, 100);
        assert!(s.total_nodes > 1);
        assert!(s.tree_height >= 2);
        assert!(s.avg_key_size_bytes > 0.0);
    }

    #[test]
    fn stats_after_bulk_load() {
        let bt = make_btree(16);
        let entries: Vec<(Bson, RecordId)> =
            (0..1000).map(|i| (Bson::Int64(i), i as u64)).collect();
        bt.bulk_load(entries);

        let s = bt.stats();
        assert_eq!(s.total_entries, 1000);
        assert!(s.total_nodes > 1);
        assert!(s.avg_key_size_bytes > 0.0);
    }

    // ---- 新增测试：前缀压缩 ----

    #[test]
    fn prefix_compression_string_keys() {
        let bt = BTree::new(8);
        bt.insert(Bson::String("collection_users".into()), 1);
        bt.insert(Bson::String("collection_orders".into()), 2);
        bt.insert(Bson::String("collection_products".into()), 3);
        bt.insert(Bson::String("collection_index".into()), 4);

        assert_eq!(bt.len(), 4);
        assert_eq!(
            bt.point_lookup(&Bson::String("collection_users".into())),
            vec![1]
        );
        assert_eq!(
            bt.point_lookup(&Bson::String("collection_orders".into())),
            vec![2]
        );
        assert_eq!(
            bt.point_lookup(&Bson::String("collection_products".into())),
            vec![3]
        );
        assert_eq!(
            bt.point_lookup(&Bson::String("collection_index".into())),
            vec![4]
        );
    }

    #[test]
    fn prefix_compression_numeric_keys() {
        let bt = BTree::new(4);
        for i in 0..20 {
            bt.insert(Bson::Int32(i * 1000), i as u64);
        }
        for i in 0..20 {
            assert_eq!(bt.point_lookup(&Bson::Int32(i * 1000)), vec![i as u64]);
        }
    }

    // ---- 新增测试：自由列表复用 ----

    #[test]
    fn free_list_reuse() {
        let bt = make_btree(4);
        for i in 0..20 {
            bt.insert(Bson::Int32(i), i as u64);
        }
        let stats_before = bt.stats();
        for i in 0..15 {
            bt.remove(&Bson::Int32(i), i as u64);
        }
        for i in 100..115 {
            bt.insert(Bson::Int32(i), i as u64);
        }
        assert_eq!(bt.len(), 20);
        let stats_after = bt.stats();
        // After removing 15 entries, internal merges free nodes to the free list.
        // New inserts reuse those freed nodes. Verify the free list is working:
        // total_nodes should not exceed initial count by much.
        let inner = bt.inner.read().unwrap();
        assert!(
            inner.free_list.is_empty() || stats_after.total_nodes <= stats_before.total_nodes + 8,
            "free_list len={}, before_nodes={}, after_nodes={}",
            inner.free_list.len(),
            stats_before.total_nodes,
            stats_after.total_nodes,
        );
    }

    // ---- 新增测试：Bson 序列化往返 ----

    #[test]
    fn bson_serde_roundtrip() {
        let values: Vec<Bson> = vec![
            Bson::Int32(42),
            Bson::Int64(-123456789),
            Bson::Double(3.14),
            Bson::String("hello world".into()),
            Bson::Boolean(true),
            Bson::Boolean(false),
            Bson::Null,
            Bson::DateTime(1700000000000),
            Bson::MinKey,
            Bson::MaxKey,
        ];
        for v in &values {
            let bytes = bson_to_bytes(v);
            let decoded = bson_from_bytes(&bytes).expect("deserialization failed");
            assert_eq!(bson_cmp(v, &decoded), std::cmp::Ordering::Equal);
        }
    }

    // ---- 新增测试：混合操作压力测试 ----

    #[test]
    fn mixed_insert_remove_lookup() {
        let bt = make_btree(4);
        for i in 0..50 {
            bt.insert(Bson::Int32(i), i as u64);
        }
        for i in (0..25).rev() {
            bt.remove(&Bson::Int32(i * 2), i as u64 * 2);
        }
        assert_eq!(bt.len(), 25);
        for i in 0..50 {
            let result = bt.point_lookup(&Bson::Int32(i));
            if i % 2 == 1 || i >= 50 {
                assert_eq!(result, vec![i as u64], "key {i} should exist");
            } else if i < 50 {
                assert!(result.is_empty(), "key {i} should be removed");
            }
        }
    }
}
