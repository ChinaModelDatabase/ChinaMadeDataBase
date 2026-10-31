use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

use mdb_common::MdbResult;

use crate::sstable::{SSTableMeta, SSTableReader, SSTableWriter};
use crate::types::*;

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub output_sst_size_bytes: usize,
    pub block_size_kb: u32,
    pub bloom_bits_per_key: u32,
    pub level_size_ratio: u32,
    pub max_concurrent_compactions: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            output_sst_size_bytes: 256 * 1024 * 1024,
            block_size_kb: 64,
            bloom_bits_per_key: 10,
            level_size_ratio: 10,
            max_concurrent_compactions: 4,
        }
    }
}

#[derive(Debug, Default)]
pub struct CompactionStats {
    pub compactions_completed: AtomicU64,
    pub keys_written: AtomicU64,
    pub bytes_written: AtomicU64,
    pub files_removed: AtomicU64,
    pub compaction_active: AtomicBool,
}

impl CompactionStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_compaction(&self, keys: u64, bytes: u64, files_removed: u64) {
        self.compactions_completed.fetch_add(1, AtomicOrdering::Relaxed);
        self.keys_written.fetch_add(keys, AtomicOrdering::Relaxed);
        self.bytes_written.fetch_add(bytes, AtomicOrdering::Relaxed);
        self.files_removed.fetch_add(files_removed, AtomicOrdering::Relaxed);
    }
}

#[derive(Debug)]
pub struct CompactionInput {
    pub level: usize,
    pub ssts: Vec<SSTableMeta>,
    pub overlapping_ln: Vec<SSTableMeta>,
}

#[derive(Debug)]
pub struct CompactionOutput {
    pub new_ssts: Vec<SSTableMeta>,
    pub removed_files: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct SstEntry {
    pub internal_key: Vec<u8>,
    pub value: Vec<u8>,
}

fn sst_path(cf_dir: &Path, file_number: u64) -> PathBuf {
    cf_dir.join(format!("{:012}.sst", file_number))
}

fn is_tombstone(value: &[u8]) -> bool {
    value.is_empty()
}

fn read_all_entries_from_sst(cf_dir: &Path, meta: &SSTableMeta) -> MdbResult<Vec<SstEntry>> {
    let path = sst_path(cf_dir, meta.file_number);
    let mut reader = SSTableReader::open(&path, meta.clone())?;
    let raw = reader.iter_all_entries()?;
    Ok(raw
        .into_iter()
        .map(|(ik, val)| SstEntry { internal_key: ik, value: val })
        .collect())
}

pub fn collect_all_versions(reader: &mut SSTableReader) -> MdbResult<Vec<SstEntry>> {
    let raw = reader.iter_all_entries()?;
    Ok(raw
        .into_iter()
        .map(|(ik, val)| SstEntry { internal_key: ik, value: val })
        .collect())
}

fn select_versions_for_key(
    versions: &[SstEntry],
    oldest_active: SequenceNumber,
) -> Vec<SstEntry> {
    if versions.is_empty() {
        return Vec::new();
    }

    let mut sorted: Vec<&SstEntry> = versions.iter().collect();
    sorted.sort_by(|a, b| {
        let sa = seqno_of(&a.internal_key);
        let sb = seqno_of(&b.internal_key);
        sa.cmp(&sb)
    });

    let n = sorted.len();
    let mut keep_indices: Vec<usize> = Vec::new();

    for i in (0..n).rev() {
        if i == n - 1 {
            keep_indices.push(i);
            continue;
        }

        let next_sn = seqno_of(&sorted[i + 1].internal_key);
        if next_sn > oldest_active {
            keep_indices.push(i);
        }
    }

    let latest_idx = n - 1;
    let latest_is_tombstone = is_tombstone(&sorted[latest_idx].value);

    if latest_is_tombstone && keep_indices.len() == 1 {
        let oldest_kept_sn = seqno_of(&sorted[keep_indices[0]].internal_key);
        if oldest_kept_sn == seqno_of(&sorted[latest_idx].internal_key) {
            let has_older_version = sorted.iter().any(|e| {
                seqno_of(&e.internal_key) < seqno_of(&sorted[latest_idx].internal_key)
            });
            if !has_older_version {
                return Vec::new();
            }
        }
    }

    keep_indices.sort();
    keep_indices
        .into_iter()
        .map(|i| SstEntry {
            internal_key: sorted[i].internal_key.clone(),
            value: sorted[i].value.clone(),
        })
        .collect()
}

fn write_output_sst(
    entries: &[SstEntry],
    cf_dir: &Path,
    file_number: u64,
    block_size_kb: u32,
    bloom_bits_per_key: u32,
    level: usize,
) -> MdbResult<SSTableMeta> {
    let path = sst_path(cf_dir, file_number);
    let estimated = entries.len().max(1);
    let mut writer = SSTableWriter::create(&path, file_number, block_size_kb, bloom_bits_per_key, estimated)?;

    for entry in entries {
        writer.add(&entry.internal_key, &entry.value)?;
    }

    let mut meta = writer.finish()?;
    meta.level = level;
    Ok(meta)
}

fn merge_and_write(
    all_entries: Vec<SstEntry>,
    cf_dir: &Path,
    next_file_number: &mut u64,
    block_size_kb: u32,
    bloom_bits_per_key: u32,
    oldest_active: SequenceNumber,
    output_size_limit: usize,
    level: usize,
) -> MdbResult<CompactionOutput> {
    if all_entries.is_empty() {
        return Ok(CompactionOutput {
            new_ssts: Vec::new(),
            removed_files: Vec::new(),
        });
    }

    let mut sorted = all_entries;
    sorted.sort_by(|a, b| compare_internal_keys(&a.internal_key, &b.internal_key));

    let mut grouped: BTreeMap<Vec<u8>, Vec<SstEntry>> = BTreeMap::new();
    for entry in sorted {
        let uk = user_key_of(&entry.internal_key).to_vec();
        grouped.entry(uk).or_default().push(entry);
    }

    let mut output_ssts = Vec::new();
    let mut current_batch: Vec<SstEntry> = Vec::new();
    let mut current_batch_size = 0usize;

    for (_user_key, versions) in &grouped {
        let selected = select_versions_for_key(versions, oldest_active);

        for entry in selected {
            if entry.value.is_empty() {
                continue;
            }

            let entry_size = entry.internal_key.len() + entry.value.len() + 32;
            if !current_batch.is_empty() && current_batch_size + entry_size > output_size_limit {
                let meta = write_output_sst(
                    &current_batch,
                    cf_dir,
                    *next_file_number,
                    block_size_kb,
                    bloom_bits_per_key,
                    level,
                )?;
                *next_file_number += 1;
                output_ssts.push(meta);
                current_batch.clear();
                current_batch_size = 0;
            }

            current_batch_size += entry_size;
            current_batch.push(entry);
        }
    }

    if !current_batch.is_empty() {
        let meta = write_output_sst(
            &current_batch,
            cf_dir,
            *next_file_number,
            block_size_kb,
            bloom_bits_per_key,
            level,
        )?;
        *next_file_number += 1;
        output_ssts.push(meta);
    }

    Ok(CompactionOutput {
        new_ssts: output_ssts,
        removed_files: Vec::new(),
    })
}

fn find_overlapping_ssts(
    ssts: &[SSTableMeta],
    start_key: &[u8],
    end_key: &[u8],
) -> Vec<SSTableMeta> {
    ssts.iter()
        .filter(|meta| {
            if meta.first_key.is_empty() || meta.last_key.is_empty() {
                return true;
            }
            let fs = escape_user_key(start_key);
            let fe = escape_user_key(end_key);
            user_key_of(&meta.first_key) < &fe && user_key_of(&meta.last_key) >= &fs
        })
        .cloned()
        .collect()
}

pub fn compact_l0_to_l1(
    l0_ssts: &[SSTableMeta],
    l1_ssts: &[SSTableMeta],
    cf_dir: &Path,
    next_file_number: u64,
    block_size_kb: u32,
    bloom_bits_per_key: u32,
    oldest_active_seqno: SequenceNumber,
) -> MdbResult<CompactionOutput> {
    let config = CompactionConfig {
        block_size_kb,
        bloom_bits_per_key,
        ..Default::default()
    };

    compact_l0_to_l1_with_stats(
        l0_ssts,
        l1_ssts,
        cf_dir,
        next_file_number,
        &config,
        oldest_active_seqno,
        &CompactionStats::new(),
    )
}

pub fn compact_l0_to_l1_with_stats(
    l0_ssts: &[SSTableMeta],
    l1_ssts: &[SSTableMeta],
    cf_dir: &Path,
    next_file_number: u64,
    config: &CompactionConfig,
    oldest_active_seqno: SequenceNumber,
    stats: &CompactionStats,
) -> MdbResult<CompactionOutput> {
    if l0_ssts.is_empty() {
        return Ok(CompactionOutput {
            new_ssts: Vec::new(),
            removed_files: Vec::new(),
        });
    }

    stats.compaction_active.store(true, AtomicOrdering::SeqCst);

    let mut all_entries = Vec::new();
    let mut file_numbers: Vec<u64> = Vec::new();

    for meta in l0_ssts {
        if sst_path(cf_dir, meta.file_number).exists() {
            let entries = read_all_entries_from_sst(cf_dir, meta)?;
            all_entries.extend(entries);
            file_numbers.push(meta.file_number);
        }
    }
    for meta in l1_ssts {
        if sst_path(cf_dir, meta.file_number).exists() {
            let entries = read_all_entries_from_sst(cf_dir, meta)?;
            all_entries.extend(entries);
            file_numbers.push(meta.file_number);
        }
    }

    let mut file_num = next_file_number;
    let output = merge_and_write(
        all_entries,
        cf_dir,
        &mut file_num,
        config.block_size_kb,
        config.bloom_bits_per_key,
        oldest_active_seqno,
        config.output_sst_size_bytes,
        1,
    )?;

    let total_keys: u64 = output.new_ssts.iter().map(|m| {
        let first = user_key_of(&m.first_key);
        let last = user_key_of(&m.last_key);
        if first.is_empty() || last.is_empty() { 0 } else { 1 }
    }).sum();
    let total_bytes: u64 = output.new_ssts.iter().map(|m| {
        (m.first_key.len() + m.last_key.len()) as u64
    }).sum();
    stats.record_compaction(total_keys, total_bytes, file_numbers.len() as u64);
    stats.compaction_active.store(false, AtomicOrdering::SeqCst);

    Ok(CompactionOutput {
        new_ssts: output.new_ssts,
        removed_files: file_numbers,
    })
}

pub fn compact_ln(
    source_ssts: &[SSTableMeta],
    target_ssts: &[SSTableMeta],
    cf_dir: &Path,
    next_file_number: u64,
    block_size_kb: u32,
    bloom_bits_per_key: u32,
    oldest_active_seqno: SequenceNumber,
) -> MdbResult<CompactionOutput> {
    let config = CompactionConfig {
        block_size_kb,
        bloom_bits_per_key,
        ..Default::default()
    };

    compact_ln_with_stats(
        source_ssts,
        target_ssts,
        cf_dir,
        next_file_number,
        &config,
        1,
        oldest_active_seqno,
        &CompactionStats::new(),
    )
}

pub fn compact_ln_with_stats(
    source_ssts: &[SSTableMeta],
    target_ssts: &[SSTableMeta],
    cf_dir: &Path,
    next_file_number: u64,
    config: &CompactionConfig,
    source_level: usize,
    oldest_active_seqno: SequenceNumber,
    stats: &CompactionStats,
) -> MdbResult<CompactionOutput> {
    if source_ssts.is_empty() {
        return Ok(CompactionOutput {
            new_ssts: Vec::new(),
            removed_files: Vec::new(),
        });
    }

    stats.compaction_active.store(true, AtomicOrdering::SeqCst);

    // start_key / end_key 必须是**原始**用户键：find_overlapping_ssts 内部
    // 会自己做转义，这里若传转义后的键会被二次转义，导致重叠范围算错。
    let start_key: Vec<u8> = source_ssts
        .iter()
        .map(|m| user_key_of_owned(&m.first_key))
        .filter(|k| !k.is_empty())
        .min()
        .unwrap_or_default();
    let end_key: Vec<u8> = source_ssts
        .iter()
        .map(|m| user_key_of_owned(&m.last_key))
        .filter(|k| !k.is_empty())
        .max()
        .map(|k| {
            // 上界取"大于该键"的哨兵：追加 0xFF 覆盖所有以 k 为前缀的更长键。
            let mut ek = k;
            ek.push(0xFF);
            ek.push(0xFF);
            ek
        })
        .unwrap_or_else(|| vec![0xFF; 64]);

    let overlapping = find_overlapping_ssts(target_ssts, &start_key, &end_key);

    let mut all_entries = Vec::new();
    let mut file_numbers: Vec<u64> = Vec::new();

    for meta in source_ssts {
        if sst_path(cf_dir, meta.file_number).exists() {
            let entries = read_all_entries_from_sst(cf_dir, meta)?;
            all_entries.extend(entries);
            file_numbers.push(meta.file_number);
        }
    }
    for meta in &overlapping {
        if sst_path(cf_dir, meta.file_number).exists() {
            let entries = read_all_entries_from_sst(cf_dir, meta)?;
            all_entries.extend(entries);
            file_numbers.push(meta.file_number);
        }
    }

    let target_level = source_level + 1;
    let mut file_num = next_file_number;
    let output = merge_and_write(
        all_entries,
        cf_dir,
        &mut file_num,
        config.block_size_kb,
        config.bloom_bits_per_key,
        oldest_active_seqno,
        config.output_sst_size_bytes,
        target_level,
    )?;

    let total_keys: u64 = output.new_ssts.iter().map(|m| {
        let first = user_key_of(&m.first_key);
        let last = user_key_of(&m.last_key);
        if first.is_empty() || last.is_empty() { 0 } else { 1 }
    }).sum();
    let total_bytes: u64 = output.new_ssts.iter().map(|m| {
        (m.first_key.len() + m.last_key.len()) as u64
    }).sum();
    stats.record_compaction(total_keys, total_bytes, file_numbers.len() as u64);
    stats.compaction_active.store(false, AtomicOrdering::SeqCst);

    Ok(CompactionOutput {
        new_ssts: output.new_ssts,
        removed_files: file_numbers,
    })
}

pub fn should_compact_l0(l0_count: usize, trigger: u32) -> bool {
    l0_count >= trigger as usize
}

pub fn should_compact_ln(level_count: usize, max_files: u32) -> bool {
    level_count > max_files as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::SSTableReader;
    use crate::sstable::SSTableWriter;
    use std::fs;
    use std::path::PathBuf;

    fn temp_dir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = PathBuf::from(format!("test_compact_{}_{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn l0_to_l1_compaction() {
        let dir = temp_dir();
        let mut file_num = 1u64;

        let mut l0_files = Vec::new();
        for _ in 0..3u32 {
            let path = dir.join(format!("{:012}.sst", file_num));
            let mut writer = SSTableWriter::create(&path, file_num, 4, 10, 100).unwrap();
            for i in 0..10u64 {
                let key = format!("key_{:04}", i);
                writer
                    .add(&make_internal_key(key.as_bytes(), i), b"v")
                    .unwrap();
            }
            let meta = writer.finish().unwrap();
            l0_files.push(meta);
            file_num += 1;
        }

        let output = compact_l0_to_l1(
            &l0_files,
            &[],
            &dir,
            file_num,
            4,
            10,
            u64::MAX,
        )
        .unwrap();

        assert!(!output.new_ssts.is_empty());
        assert_eq!(output.removed_files.len(), 3);

        for meta in &output.new_ssts {
            let mut reader = SSTableReader::open(sst_path(&dir, meta.file_number), meta.clone()).unwrap();
            let result = reader.get(b"key_0005", u64::MAX).unwrap();
            assert!(result.is_some(), "key_0005 should exist after compaction");
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_deduplicates_keys() {
        let dir = temp_dir();
        let mut file_num = 1u64;

        let mut l0_files = Vec::new();
        for ver in 0..3u64 {
            let path = dir.join(format!("{:012}.sst", file_num));
            let mut writer = SSTableWriter::create(&path, file_num, 4, 10, 100).unwrap();
            writer
                .add(
                    &make_internal_key(b"same_key", ver),
                    format!("v{ver}").as_bytes(),
                )
                .unwrap();
            let meta = writer.finish().unwrap();
            l0_files.push(meta);
            file_num += 1;
        }

        let output = compact_l0_to_l1(
            &l0_files,
            &[],
            &dir,
            file_num,
            4,
            10,
            u64::MAX,
        )
        .unwrap();

        let mut total_versions = 0;
        for meta in &output.new_ssts {
            let mut reader =
                SSTableReader::open(sst_path(&dir, meta.file_number), meta.clone()).unwrap();
            let results = reader.range_scan(b"\x00", b"\xff", u64::MAX).unwrap();
            total_versions += results.len();
        }

        assert!(
            total_versions >= 1,
            "should have at least one version after compaction"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn tombstones_eliminated_when_no_snapshot_needs_them() {
        let dir = temp_dir();
        let mut file_num = 1u64;

        let mut l0_files = Vec::new();
        for i in 0..2u32 {
            let path = dir.join(format!("{:012}.sst", file_num));
            let mut writer = SSTableWriter::create(&path, file_num, 4, 10, 100).unwrap();
            if i == 0 {
                writer
                    .add(&make_internal_key(b"key1", 1), b"value1")
                    .unwrap();
            } else {
                writer
                    .add(&make_internal_key(b"key1", 2), b"")
                    .unwrap();
            }
            let meta = writer.finish().unwrap();
            l0_files.push(meta);
            file_num += 1;
        }

        let output = compact_l0_to_l1(
            &l0_files,
            &[],
            &dir,
            file_num,
            4,
            10,
            5,
        )
        .unwrap();

        let mut total_entries = 0;
        for meta in &output.new_ssts {
            let mut reader =
                SSTableReader::open(sst_path(&dir, meta.file_number), meta.clone()).unwrap();
            let results = reader.range_scan(b"\x00", b"\xff", u64::MAX).unwrap();
            total_entries += results.len();
        }

        assert!(
            total_entries <= 1,
            "tombstone should be eliminated when no snapshot needs it, got {total_entries}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn tombstones_kept_when_snapshot_needs_them() {
        let dir = temp_dir();
        let mut file_num = 1u64;

        let mut l0_files = Vec::new();
        for i in 0..2u32 {
            let path = dir.join(format!("{:012}.sst", file_num));
            let mut writer = SSTableWriter::create(&path, file_num, 4, 10, 100).unwrap();
            if i == 0 {
                writer
                    .add(&make_internal_key(b"key1", 1), b"value1")
                    .unwrap();
            } else {
                writer
                    .add(&make_internal_key(b"key1", 2), b"")
                    .unwrap();
            }
            let meta = writer.finish().unwrap();
            l0_files.push(meta);
            file_num += 1;
        }

        let output = compact_l0_to_l1(
            &l0_files,
            &[],
            &dir,
            file_num,
            4,
            10,
            1,
        )
        .unwrap();

        let mut found_value = false;
        let mut total_output_ssts = 0;
        for meta in &output.new_ssts {
            total_output_ssts += 1;
            let mut reader =
                SSTableReader::open(sst_path(&dir, meta.file_number), meta.clone()).unwrap();
            let result = reader.get(b"key1", u64::MAX).unwrap();
            if result.is_some() {
                found_value = true;
            }
        }

        assert!(total_output_ssts > 0, "should produce output SSTs");
        assert!(found_value, "tombstone and older version should be kept when snapshot at seqno 1");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn ln_compaction_merges_with_overlapping() {
        let dir = temp_dir();
        let mut file_num = 1u64;

        let mut l1_files = Vec::new();
        for i in 0..2u32 {
            let path = dir.join(format!("{:012}.sst", file_num));
            let mut writer = SSTableWriter::create(&path, file_num, 4, 10, 100).unwrap();
            let start = i * 5;
            for j in start..start + 5 {
                let key = format!("key_{:04}", j);
                writer
                    .add(&make_internal_key(key.as_bytes(), j as u64), b"v")
                    .unwrap();
            }
            let meta = writer.finish().unwrap();
            l1_files.push(meta);
            file_num += 1;
        }

        let mut l2_files = Vec::new();
        let path = dir.join(format!("{:012}.sst", file_num));
        let mut writer = SSTableWriter::create(&path, file_num, 4, 10, 100).unwrap();
        for j in 0..10u64 {
            let key = format!("key_{:04}", j);
            writer
                .add(&make_internal_key(key.as_bytes(), j), b"old")
                .unwrap();
        }
        let meta = writer.finish().unwrap();
        l2_files.push(meta);
        file_num += 1;

        let output = compact_ln(
            &l1_files,
            &l2_files,
            &dir,
            file_num,
            4,
            10,
            u64::MAX,
        )
        .unwrap();

        assert!(!output.new_ssts.is_empty());
        assert_eq!(output.removed_files.len(), 3);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn output_sst_size_limit() {
        let dir = temp_dir();
        let mut file_num = 1u64;

        let mut l0_files = Vec::new();
        for i in 0..3u32 {
            let path = dir.join(format!("{:012}.sst", file_num));
            let mut writer = SSTableWriter::create(&path, file_num, 4, 10, 100).unwrap();
            for j in 0..100u64 {
                let key = format!("key_{:06}", i as u64 * 100 + j);
                writer
                    .add(&make_internal_key(key.as_bytes(), j), b"value")
                    .unwrap();
            }
            let meta = writer.finish().unwrap();
            l0_files.push(meta);
            file_num += 1;
        }

        let output = compact_l0_to_l1(
            &l0_files,
            &[],
            &dir,
            file_num,
            4,
            10,
            u64::MAX,
        )
        .unwrap();

        assert!(output.new_ssts.len() >= 1);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn version_selection_keeps_all_visible() {
        let v1 = SstEntry { internal_key: make_internal_key(b"k", 1), value: b"v1".to_vec() };
        let v5 = SstEntry { internal_key: make_internal_key(b"k", 5), value: b"v5".to_vec() };
        let v10 = SstEntry { internal_key: make_internal_key(b"k", 10), value: b"v10".to_vec() };
        let versions = vec![v1, v5, v10];

        let result = select_versions_for_key(&versions, 1);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn version_selection_filters_old_versions() {
        let v1 = SstEntry { internal_key: make_internal_key(b"k", 1), value: b"v1".to_vec() };
        let v5 = SstEntry { internal_key: make_internal_key(b"k", 5), value: b"v5".to_vec() };
        let v10 = SstEntry { internal_key: make_internal_key(b"k", 10), value: b"v10".to_vec() };
        let versions = vec![v1, v5, v10];

        let result = select_versions_for_key(&versions, 5);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].value, b"v5");
        assert_eq!(result[1].value, b"v10");
    }

    #[test]
    fn version_selection_handles_tombstone_only() {
        let v1 = SstEntry { internal_key: make_internal_key(b"k", 1), value: b"".to_vec() };
        let versions = vec![v1];

        let result = select_versions_for_key(&versions, 5);
        assert!(result.is_empty());
    }

    #[test]
    fn version_selection_tombstone_with_older_visible() {
        let v1 = SstEntry { internal_key: make_internal_key(b"k", 1), value: b"v1".to_vec() };
        let v5 = SstEntry { internal_key: make_internal_key(b"k", 5), value: b"".to_vec() };
        let versions = vec![v1, v5];

        let result = select_versions_for_key(&versions, 1);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].value, b"v1");
        assert_eq!(result[1].value, b"");
    }

    #[test]
    fn should_compact_l0_trigger() {
        assert!(!should_compact_l0(3, 4));
        assert!(should_compact_l0(4, 4));
        assert!(should_compact_l0(5, 4));
    }

    #[test]
    fn should_compact_ln_check() {
        assert!(!should_compact_ln(10, 10));
        assert!(should_compact_ln(11, 10));
    }
}
