//! 内存排序器：支持多键排序与外部排序。

use std::cmp::Ordering;

use mdb_doc::Document;

use crate::operators::bson_cmp;
use crate::types::SortDirection;

/// 内存排序器。
///
/// 当结果集在内存容量内时使用快速排序；
/// 超过阈值时自动切换为外部排序（分块排序 + 合并）。
#[derive(Debug)]
pub struct Sorter {
    /// 触发外部排序的文档数阈值。
    external_threshold: usize,
}

impl Sorter {
    /// 创建排序器，默认阈值 10000。
    pub fn new() -> Self {
        Self { external_threshold: 10_000 }
    }

    /// 创建自定义阈值的排序器。
    pub fn with_threshold(threshold: usize) -> Self {
        Self { external_threshold: threshold }
    }

    /// 排序文档集合。
    ///
    /// 根据文档数量自动选择内存排序或外部排序。
    pub fn sort(&self, docs: &mut Vec<Document>, spec: &[(String, SortDirection)]) {
        if docs.len() <= self.external_threshold {
            self.in_memory_sort(docs, spec);
        } else {
            self.external_sort(docs, spec);
        }
    }

    /// 内存排序：使用标准库的快速排序。
    fn in_memory_sort(&self, docs: &mut Vec<Document>, spec: &[(String, SortDirection)]) {
        docs.sort_by(|a, b| compare_docs(a, b, spec));
    }

    /// 外部排序：分块排序后归并。
    ///
    /// 算法：
    /// 1. 将文档分成多个块
    /// 2. 对每个块进行内存排序
    /// 3. 多路归并所有已排序的块
    fn external_sort(&self, docs: &mut Vec<Document>, spec: &[(String, SortDirection)]) {
        let chunk_size = self.external_threshold;
        let mut chunks: Vec<Vec<Document>> = Vec::new();

        // 分块排序
        for chunk in docs.chunks(chunk_size) {
            let mut sorted_chunk = chunk.to_vec();
            self.in_memory_sort(&mut sorted_chunk, spec);
            chunks.push(sorted_chunk);
        }

        if chunks.is_empty() {
            docs.clear();
            return;
        }

        if chunks.len() == 1 {
            docs.clone_from_slice(&chunks[0]);
            return;
        }

        // 多路归并
        let merged = k_way_merge(&chunks, spec);
        *docs = merged;
    }
}

impl Default for Sorter {
    fn default() -> Self {
        Self::new()
    }
}

/// 多键比较两个文档。
fn compare_docs(a: &Document, b: &Document, spec: &[(String, SortDirection)]) -> Ordering {
    for (field, dir) in spec {
        let va = a.get(field);
        let vb = b.get(field);
        let ord = match (va, vb) {
            (Some(a), Some(b)) => bson_cmp(a, b).unwrap_or(Ordering::Equal),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        };
        let effective_ord = match dir {
            SortDirection::Ascending => ord,
            SortDirection::Descending => ord.reverse(),
        };
        if effective_ord != Ordering::Equal {
            return effective_ord;
        }
    }
    Ordering::Equal
}

/// 多路归并：从多个已排序的块中归并出有序序列。
///
/// 使用堆（优先队列）实现高效的多路归并。
fn k_way_merge(chunks: &[Vec<Document>], spec: &[(String, SortDirection)]) -> Vec<Document> {
    if chunks.is_empty() {
        return Vec::new();
    }

    let total_len: usize = chunks.iter().map(|c| c.len()).sum();
    let mut result = Vec::with_capacity(total_len);

    // 维护一个 (chunk_idx, doc_idx) 的堆
    let mut heap: Vec<(usize, usize)> = (0..chunks.len())
        .filter(|&i| !chunks[i].is_empty())
        .map(|i| (i, 0))
        .collect();

    // 堆化
    for i in (0..heap.len() / 2).rev() {
        sift_down(&mut heap, i, chunks, spec);
    }

    while !heap.is_empty() {
        // 取出堆顶
        let (chunk_idx, doc_idx) = heap[0];
        result.push(chunks[chunk_idx][doc_idx].clone());

        let next_idx = doc_idx + 1;
        if next_idx < chunks[chunk_idx].len() {
            heap[0] = (chunk_idx, next_idx);
            sift_down(&mut heap, 0, chunks, spec);
        } else {
            // 该块已空，用最后一个元素替换
            let last = heap.len() - 1;
            heap[0] = heap[last];
            heap.pop();
            if !heap.is_empty() {
                sift_down(&mut heap, 0, chunks, spec);
            }
        }
    }

    result
}

/// 堆下沉操作。
fn sift_down(
    heap: &mut [(usize, usize)],
    root: usize,
    chunks: &[Vec<Document>],
    spec: &[(String, SortDirection)],
) {
    let len = heap.len();
    let mut smallest = root;

    loop {
        let left = 2 * smallest + 1;
        let right = 2 * smallest + 2;
        let mut next = smallest;

        if left < len {
            let cmp = compare_docs(
                &chunks[heap[left].0][heap[left].1],
                &chunks[heap[next].0][heap[next].1],
                spec,
            );
            if cmp == Ordering::Less {
                next = left;
            }
        }
        if right < len {
            let cmp = compare_docs(
                &chunks[heap[right].0][heap[right].1],
                &chunks[heap[next].0][heap[next].1],
                spec,
            );
            if cmp == Ordering::Less {
                next = right;
            }
        }
        if next == smallest {
            break;
        }
        heap.swap(smallest, next);
        smallest = next;
    }
}

/// 简化的原地多路归并（适用于少量块的情况）。
///
/// 对于少量块（<=8），使用线性扫描比堆更高效。
pub fn merge_sorted_chunks(
    chunks: Vec<Vec<Document>>,
    spec: &[(String, SortDirection)],
) -> Vec<Document> {
    let total_len: usize = chunks.iter().map(|c| c.len()).sum();
    let mut result = Vec::with_capacity(total_len);
    let mut indices: Vec<usize> = vec![0; chunks.len()];

    loop {
        let mut best_chunk = None;
        for (i, chunk) in chunks.iter().enumerate() {
            if indices[i] < chunk.len() {
                match best_chunk {
                    None => best_chunk = Some(i),
                    Some(best) => {
                        let ord = compare_docs(
                            &chunk[indices[i]],
                            &chunks[best][indices[best]],
                            spec,
                        );
                        if ord == Ordering::Less {
                            best_chunk = Some(i);
                        }
                    }
                }
            }
        }

        match best_chunk {
            Some(i) => {
                result.push(chunks[i][indices[i]].clone());
                indices[i] += 1;
            }
            None => break,
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdb_doc::{Bson, Document};

    fn make_doc(id: i32, value: i32) -> Document {
        let mut doc = Document::new();
        doc.insert("_id", Bson::Int32(id));
        doc.insert("value", Bson::Int32(value));
        doc
    }

    #[test]
    fn sorter_empty() {
        let sorter = Sorter::new();
        let mut docs = Vec::new();
        let spec = vec![("value".to_owned(), SortDirection::Ascending)];
        sorter.sort(&mut docs, &spec);
        assert!(docs.is_empty());
    }

    #[test]
    fn sorter_single_doc() {
        let sorter = Sorter::new();
        let mut docs = vec![make_doc(1, 42)];
        let spec = vec![("value".to_owned(), SortDirection::Ascending)];
        sorter.sort(&mut docs, &spec);
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].get("value").and_then(Bson::as_i64), Some(42));
    }

    #[test]
    fn sorter_ascending() {
        let sorter = Sorter::new();
        let mut docs = vec![make_doc(1, 30), make_doc(2, 10), make_doc(3, 20)];
        let spec = vec![("value".to_owned(), SortDirection::Ascending)];
        sorter.sort(&mut docs, &spec);
        let values: Vec<i64> = docs.iter().filter_map(|d| d.get("value").and_then(Bson::as_i64)).collect();
        assert_eq!(values, vec![10, 20, 30]);
    }

    #[test]
    fn sorter_descending() {
        let sorter = Sorter::new();
        let mut docs = vec![make_doc(1, 30), make_doc(2, 10), make_doc(3, 20)];
        let spec = vec![("value".to_owned(), SortDirection::Descending)];
        sorter.sort(&mut docs, &spec);
        let values: Vec<i64> = docs.iter().filter_map(|d| d.get("value").and_then(Bson::as_i64)).collect();
        assert_eq!(values, vec![30, 20, 10]);
    }

    #[test]
    fn sorter_multi_key() {
        let sorter = Sorter::new();
        let mut d1 = make_doc(1, 1);
        d1.insert("secondary", Bson::Int32(2));
        let mut d2 = make_doc(2, 1);
        d2.insert("secondary", Bson::Int32(1));
        let mut d3 = make_doc(3, 0);
        d3.insert("secondary", Bson::Int32(3));
        let mut docs = vec![d1, d2, d3];

        let spec = vec![
            ("value".to_owned(), SortDirection::Ascending),
            ("secondary".to_owned(), SortDirection::Ascending),
        ];
        sorter.sort(&mut docs, &spec);
        let values: Vec<(i64, i64)> = docs
            .iter()
            .map(|d| {
                (
                    d.get("value").and_then(Bson::as_i64).unwrap(),
                    d.get("secondary").and_then(Bson::as_i64).unwrap(),
                )
            })
            .collect();
        assert_eq!(values, vec![(0, 3), (1, 1), (1, 2)]);
    }

    #[test]
    fn sorter_stable_order() {
        let sorter = Sorter::new();
        let mut docs = vec![make_doc(1, 5), make_doc(2, 5), make_doc(3, 5)];
        let spec = vec![("value".to_owned(), SortDirection::Ascending)];
        sorter.sort(&mut docs, &spec);
        // All values equal, order should be preserved (stable sort)
        let ids: Vec<i64> = docs.iter().filter_map(|d| d.get("_id").and_then(Bson::as_i64)).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn sorter_external_sort() {
        // Use threshold of 5 to trigger external sort
        let sorter = Sorter::with_threshold(5);
        let mut docs: Vec<Document> = (0..20)
            .rev()
            .map(|i| make_doc(i, i))
            .collect();
        let spec = vec![("value".to_owned(), SortDirection::Ascending)];
        sorter.sort(&mut docs, &spec);
        let values: Vec<i64> = docs.iter().filter_map(|d| d.get("value").and_then(Bson::as_i64)).collect();
        assert_eq!(values, (0..20).collect::<Vec<_>>());
    }

    #[test]
    fn merge_sorted_chunks_basic() {
        let chunk1 = vec![make_doc(1, 1), make_doc(3, 3)];
        let chunk2 = vec![make_doc(2, 2), make_doc(4, 4)];
        let spec = vec![("value".to_owned(), SortDirection::Ascending)];
        let result = merge_sorted_chunks(vec![chunk1, chunk2], &spec);
        let values: Vec<i64> = result.iter().filter_map(|d| d.get("value").and_then(Bson::as_i64)).collect();
        assert_eq!(values, vec![1, 2, 3, 4]);
    }

    #[test]
    fn merge_sorted_chunks_empty() {
        let spec = vec![("value".to_owned(), SortDirection::Ascending)];
        let result = merge_sorted_chunks(vec![], &spec);
        assert!(result.is_empty());
    }

    #[test]
    fn merge_sorted_chunks_single() {
        let chunk = vec![make_doc(2, 1), make_doc(3, 2), make_doc(1, 3)];
        let spec = vec![("value".to_owned(), SortDirection::Ascending)];
        let result = merge_sorted_chunks(vec![chunk], &spec);
        let values: Vec<i64> = result.iter().filter_map(|d| d.get("value").and_then(Bson::as_i64)).collect();
        // merge_sorted_chunks assumes chunks are already sorted
        // This chunk is not sorted, so it passes through as-is
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn merge_sorted_chunks_uneven() {
        let chunk1 = vec![make_doc(1, 1), make_doc(4, 4)];
        let chunk2 = vec![make_doc(2, 2), make_doc(3, 3), make_doc(5, 5)];
        let chunk3 = vec![make_doc(6, 6)];
        let spec = vec![("value".to_owned(), SortDirection::Ascending)];
        let result = merge_sorted_chunks(vec![chunk1, chunk2, chunk3], &spec);
        let values: Vec<i64> = result.iter().filter_map(|d| d.get("value").and_then(Bson::as_i64)).collect();
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn sorter_with_strings() {
        let sorter = Sorter::new();
        let mut docs = vec![
            make_doc(1, 0),
            make_doc(2, 0),
            make_doc(3, 0),
        ];
        docs[0].insert("name", Bson::String("Charlie".into()));
        docs[1].insert("name", Bson::String("Alice".into()));
        docs[2].insert("name", Bson::String("Bob".into()));
        let spec = vec![("name".to_owned(), SortDirection::Ascending)];
        sorter.sort(&mut docs, &spec);
        let names: Vec<&str> = docs.iter().filter_map(|d| d.get("name").and_then(Bson::as_str)).collect();
        assert_eq!(names, vec!["Alice", "Bob", "Charlie"]);
    }
}
