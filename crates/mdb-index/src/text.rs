//! 文本索引：倒排索引 + TF-IDF 排序。
//!
//! - Tokenizer：按空白/标点切分，支持中英文
//! - Stop word 过滤
//! - TF-IDF 排序

use std::collections::HashMap;

use crate::types::RecordId;

/// 倒排索引条目。
#[derive(Debug, Clone)]
struct InvertedEntry {
    records: Vec<(RecordId, f64)>, // (record_id, tf)
    doc_count: usize,              // 包含此 token 的文档数
}

/// 文本索引。
#[derive(Debug)]
pub struct TextIndex {
    /// token → 倒排列表
    index: HashMap<String, InvertedEntry>,
    /// 总文档数
    total_docs: usize,
    name: String,
}

impl TextIndex {
    pub fn new(name: impl Into<String>) -> Self {
        Self { index: HashMap::new(), total_docs: 0, name: name.into() }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn len(&self) -> usize {
        self.total_docs
    }

    pub fn is_empty(&self) -> bool {
        self.total_docs == 0
    }

    /// 对文本进行分词。
    pub fn tokenize(text: &str) -> Vec<String> {
        text.split(|c: char| {
            c.is_whitespace() || c == '.' || c == ',' || c == ';' || c == ':'
                || c == '!' || c == '?' || c == '(' || c == ')' || c == '"'
                || c == '\'' || c == '-' || c == '/' || c == '\\' || c == '@'
                || c == '#' || c == '$' || c == '%' || c == '&' || c == '*'
                || c == '+' || c == '=' || c == '<' || c == '>' || c == '|'
                || c == '{' || c == '}' || c == '[' || c == ']' || c == '^'
                || c == '`' || c == '~'
        })
        .filter(|s| !s.is_empty())
        .map(|s| s.to_lowercase())
        .filter(|s| !Self::is_stop_word(s))
        .collect()
    }

    /// 判断是否为停用词（常见英文 + 常见中文虚词）。
    fn is_stop_word(word: &str) -> bool {
        matches!(
            word,
            "the" | "a" | "an" | "is" | "are" | "was" | "were" | "be" | "been"
            | "being" | "have" | "has" | "had" | "do" | "does" | "did" | "will"
            | "would" | "could" | "should" | "may" | "might" | "shall" | "can"
            | "of" | "at" | "by" | "for" | "with" | "about" | "between"
            | "through" | "during" | "before" | "after" | "above" | "below"
            | "to" | "from" | "up" | "down" | "in" | "out" | "on" | "off"
            | "over" | "under" | "again" | "further" | "then" | "once"
            | "and" | "but" | "or" | "nor" | "not" | "so" | "very"
            | "just" | "than" | "too" | "also" | "this" | "that" | "these"
            | "those" | "i" | "me" | "my" | "we" | "our" | "you" | "your"
            | "he" | "him" | "his" | "she" | "her" | "it" | "its" | "they"
            | "them" | "their" | "what" | "which" | "who" | "whom"
            // 中文常见虚词
            | "的" | "了" | "在" | "是" | "我" | "有" | "和" | "就" | "不"
            | "人" | "都" | "一" | "一个" | "上" | "也" | "很" | "到" | "说"
            | "要" | "去" | "你" | "会" | "着" | "没有" | "看" | "好" | "自己"
        )
    }

    /// 插入一个文档的文本内容。
    pub fn insert(&mut self, text: &str, record_id: RecordId) {
        let tokens = Self::tokenize(text);
        let token_count = tokens.len() as f64;
        if token_count == 0.0 {
            return;
        }

        // 统计每个 token 的出现次数
        let mut freq: HashMap<String, usize> = HashMap::new();
        for token in &tokens {
            *freq.entry(token.clone()).or_insert(0) += 1;
        }

        // 计算 TF 并更新倒排索引
        for (token, count) in freq {
            let tf = count as f64 / token_count;
            let entry = self.index.entry(token).or_insert_with(|| InvertedEntry {
                records: vec![],
                doc_count: 0,
            });
            // 检查是否已有此 record_id
            if !entry.records.iter().any(|(rid, _)| *rid == record_id) {
                entry.records.push((record_id, tf));
                entry.doc_count += 1;
            }
        }

        self.total_docs += 1;
    }

    /// 移除指定 record_id 的所有倒排条目。
    pub fn remove(&mut self, record_id: RecordId) {
        for entry in self.index.values_mut() {
            entry.records.retain(|(rid, _)| *rid != record_id);
            // 注意: doc_count 不减，因为我们无法区分是新增还是更新
        }
        if self.total_docs > 0 {
            self.total_docs -= 1;
        }
    }

    /// TF-IDF 搜索：返回按相关性排序的 (record_id, score)。
    pub fn search(&self, query: &str) -> Vec<(RecordId, f64)> {
        let tokens = Self::tokenize(query);
        if tokens.is_empty() {
            return vec![];
        }

        // 对每个 token 计算 IDF
        let mut scores: HashMap<RecordId, f64> = HashMap::new();

        for token in &tokens {
            if let Some(entry) = self.index.get(token) {
                // IDF = log(N / df)，N 为总文档数，df 为包含此 token 的文档数
                let idf = if entry.doc_count > 0 {
                    (self.total_docs as f64 / entry.doc_count as f64).ln()
                } else {
                    0.0
                };

                for (record_id, tf) in &entry.records {
                    let tfidf = tf * idf;
                    *scores.entry(*record_id).or_insert(0.0) += tfidf;
                }
            }
        }

        // 按分数降序排列
        let mut results: Vec<(RecordId, f64)> = scores.into_iter().collect();
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_basic() {
        let tokens = TextIndex::tokenize("Hello, World! This is a test.");
        let expected = vec!["hello", "world", "test"];
        assert_eq!(tokens, expected);
    }

    #[test]
    fn tokenize_chinese() {
        let tokens = TextIndex::tokenize("数据库 索引 是 什么");
        // "是" 和 "什么" 都不是 stop word
        assert!(tokens.contains(&"数据库".to_string()));
        assert!(tokens.contains(&"索引".to_string()));
    }

    #[test]
    fn stop_words_filtered() {
        let tokens = TextIndex::tokenize("the quick brown fox");
        assert!(!tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
        assert!(tokens.contains(&"brown".to_string()));
        assert!(tokens.contains(&"fox".to_string()));
    }

    #[test]
    fn insert_and_search() {
        let mut idx = TextIndex::new("text_test");
        idx.insert("the quick brown fox jumps over the lazy dog", 1);
        idx.insert("the lazy dog sleeps in the sun", 2);
        idx.insert("quick brown fox is fast", 3);

        let results = idx.search("quick fox");
        assert!(!results.is_empty());
        // record_id 1 和 3 包含 "quick" 和 "fox"
        let rids: Vec<RecordId> = results.iter().map(|(r, _)| *r).collect();
        assert!(rids.contains(&1));
        assert!(rids.contains(&3));
    }

    #[test]
    fn search_empty_query() {
        let idx = TextIndex::new("text_test");
        assert!(idx.search("").is_empty());
    }

    #[test]
    fn search_no_match() {
        let mut idx = TextIndex::new("text_test");
        idx.insert("hello world", 1);
        assert!(idx.search("xyz").is_empty());
    }

    #[test]
    fn remove_record() {
        let mut idx = TextIndex::new("text_test");
        idx.insert("hello world", 1);
        idx.insert("hello there", 2);
        idx.remove(1);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn tfidf_ranking() {
        let mut idx = TextIndex::new("text_test");
        // "rust" 出现在 doc 1 中 3 次，doc 2 中 1 次
        idx.insert("rust rust rust programming", 1);
        idx.insert("rust programming language", 2);
        idx.insert("python programming language", 3);

        let results = idx.search("rust");
        assert!(!results.is_empty());
        // doc 1 的 TF 更高，应该排在前面
        assert_eq!(results[0].0, 1);
    }
}
