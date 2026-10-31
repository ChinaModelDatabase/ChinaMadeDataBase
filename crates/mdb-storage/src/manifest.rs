use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write, BufWriter};
use std::path::{Path, PathBuf};

use mdb_common::MdbResult;

use crate::error::*;
use crate::sstable::SSTableMeta;
use crate::types::*;

const MANIFEST_MAGIC: u64 = 0x4D_44_42_4D_41_4E_49_46;
const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct CfManifest {
    pub cf_id: ColumnFamilyId,
    pub name: String,
    pub files: Vec<SSTableMeta>,
}

#[derive(Debug, Clone)]
pub struct ManifestData {
    pub cfs: HashMap<ColumnFamilyId, CfManifest>,
    pub next_file_number: u64,
    pub last_seqno: SequenceNumber,
}

impl ManifestData {
    pub fn new() -> Self {
        Self {
            cfs: HashMap::new(),
            next_file_number: 1,
            last_seqno: 0,
        }
    }
}

#[derive(Debug)]
pub struct Manifest {
    dir: PathBuf,
    current_number: u64,
    data: ManifestData,
}

impl Manifest {
    pub fn open(dir: impl Into<PathBuf>) -> MdbResult<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|e| io_err(format!("create manifest dir: {e}")))?;

        let current_path = dir.join("CURRENT");
        if current_path.exists() {
            let content = fs::read_to_string(&current_path)
                .map_err(|e| io_err(format!("read CURRENT: {e}")))?;
            let manifest_name = content.trim();
            if let Some(num_str) = manifest_name.strip_prefix("MANIFEST-") {
                if let Ok(num) = num_str.parse::<u64>() {
                    let manifest_path = dir.join(manifest_name);
                    if manifest_path.exists() {
                        let data = Self::read_manifest(&manifest_path)?;
                        return Ok(Self { dir, current_number: num, data });
                    }
                }
            }
        }

        let manifest = Self { dir, current_number: 0, data: ManifestData::new() };
        Ok(manifest)
    }

    pub fn data(&self) -> &ManifestData {
        &self.data
    }

    pub fn data_mut(&mut self) -> &mut ManifestData {
        &mut self.data
    }

    pub fn next_file_number(&self) -> u64 {
        self.data.next_file_number
    }

    pub fn allocate_file_number(&mut self) -> u64 {
        let n = self.data.next_file_number;
        self.data.next_file_number += 1;
        n
    }

    pub fn set_last_seqno(&mut self, seqno: SequenceNumber) {
        if seqno > self.data.last_seqno {
            self.data.last_seqno = seqno;
        }
    }

    pub fn add_sst(&mut self, cf_id: ColumnFamilyId, meta: SSTableMeta) {
        self.data.cfs.entry(cf_id).or_insert_with(|| CfManifest {
            cf_id,
            name: String::new(),
            files: Vec::new(),
        }).files.push(meta);
    }

    pub fn remove_ssts(&mut self, cf_id: ColumnFamilyId, file_numbers: &[u64]) {
        if let Some(cf) = self.data.cfs.get_mut(&cf_id) {
            cf.files.retain(|f| !file_numbers.contains(&f.file_number));
        }
    }

    pub fn get_cf(&self, cf_id: ColumnFamilyId) -> Option<&CfManifest> {
        self.data.cfs.get(&cf_id)
    }

    pub fn get_cf_mut(&mut self, cf_id: ColumnFamilyId) -> Option<&mut CfManifest> {
        self.data.cfs.get_mut(&cf_id)
    }

    pub fn cf_ids(&self) -> Vec<ColumnFamilyId> {
        self.data.cfs.keys().copied().collect()
    }

    pub fn flush(&mut self) -> MdbResult<()> {
        let new_number = self.current_number + 1;
        let manifest_name = format!("MANIFEST-{:06}", new_number);
        let manifest_path = self.dir.join(&manifest_name);

        {
            let file = File::create(&manifest_path)
                .map_err(|e| io_err(format!("create MANIFEST: {e}")))?;
            let mut writer = BufWriter::new(file);
            Self::write_manifest_data(&mut writer, &self.data)?;
            writer.flush().map_err(|e| io_err(format!("flush MANIFEST: {e}")))?;
            let file = writer.into_inner().map_err(|e| io_err(format!("into_inner: {e}")))?;
            file.sync_all().map_err(|e| io_err(format!("fsync MANIFEST: {e}")))?;
        }

        let tmp_path = self.dir.join("CURRENT.tmp");
        {
            let mut file = File::create(&tmp_path)
                .map_err(|e| io_err(format!("create CURRENT.tmp: {e}")))?;
            write!(file, "{}\n", manifest_name)
                .map_err(|e| io_err(format!("write CURRENT.tmp: {e}")))?;
            file.sync_all().map_err(|e| io_err(format!("fsync CURRENT.tmp: {e}")))?;
        }

        fs::rename(&tmp_path, self.dir.join("CURRENT"))
            .map_err(|e| io_err(format!("rename CURRENT: {e}")))?;

        #[cfg(not(windows))]
        {
            let dir_file = File::open(&self.dir)
                .map_err(|e| io_err(format!("open dir for fsync: {e}")))?;
            dir_file.sync_all().map_err(|e| io_err(format!("fsync dir: {e}")))?;
        }

        self.current_number = new_number;

        let old_manifest = self.dir.join(format!("MANIFEST-{:06}", new_number - 1));
        if old_manifest.exists() && new_number > 1 {
            let _ = fs::remove_file(&old_manifest);
        }

        Ok(())
    }

    fn write_manifest_data(writer: &mut impl Write, data: &ManifestData) -> MdbResult<()> {
        writer.write_all(&MANIFEST_MAGIC.to_be_bytes())
            .map_err(|e| io_err(format!("write magic: {e}")))?;
        writer.write_all(&MANIFEST_VERSION.to_be_bytes())
            .map_err(|e| io_err(format!("write version: {e}")))?;
        writer.write_all(&(data.cfs.len() as u32).to_be_bytes())
            .map_err(|e| io_err(format!("write num_cfs: {e}")))?;

        for (cf_id, cf) in &data.cfs {
            writer.write_all(&cf_id.to_be_bytes())
                .map_err(|e| io_err(format!("write cf_id: {e}")))?;
            writer.write_all(&(cf.name.len() as u32).to_be_bytes())
                .map_err(|e| io_err(format!("write cf_name_len: {e}")))?;
            writer.write_all(cf.name.as_bytes())
                .map_err(|e| io_err(format!("write cf_name: {e}")))?;
            writer.write_all(&(cf.files.len() as u32).to_be_bytes())
                .map_err(|e| io_err(format!("write num_files: {e}")))?;

            for file in &cf.files {
                writer.write_all(&file.file_number.to_be_bytes())
                    .map_err(|e| io_err(format!("write file_number: {e}")))?;
                writer.write_all(&file.seqno_min.to_be_bytes())
                    .map_err(|e| io_err(format!("write seqno_min: {e}")))?;
                writer.write_all(&file.seqno_max.to_be_bytes())
                    .map_err(|e| io_err(format!("write seqno_max: {e}")))?;
                writer.write_all(&(file.first_key.len() as u32).to_be_bytes())
                    .map_err(|e| io_err(format!("write first_key_len: {e}")))?;
                writer.write_all(&file.first_key)
                    .map_err(|e| io_err(format!("write first_key: {e}")))?;
                writer.write_all(&(file.last_key.len() as u32).to_be_bytes())
                    .map_err(|e| io_err(format!("write last_key_len: {e}")))?;
                writer.write_all(&file.last_key)
                    .map_err(|e| io_err(format!("write last_key: {e}")))?;
                writer.write_all(&(file.level as u32).to_be_bytes())
                    .map_err(|e| io_err(format!("write level: {e}")))?;
            }
        }

        writer.write_all(&data.next_file_number.to_be_bytes())
            .map_err(|e| io_err(format!("write next_file_number: {e}")))?;
        writer.write_all(&data.last_seqno.to_be_bytes())
            .map_err(|e| io_err(format!("write last_seqno: {e}")))?;

        Ok(())
    }

    fn read_manifest(path: &Path) -> MdbResult<ManifestData> {
        let mut file = File::open(path).map_err(|e| io_err(format!("open MANIFEST: {e}")))?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)
            .map_err(|e| io_err(format!("read MANIFEST: {e}")))?;

        if content.len() < 12 {
            return Err(corrupt_err("MANIFEST too small"));
        }

        let magic = u64::from_be_bytes(content[0..8].try_into().unwrap());
        if magic != MANIFEST_MAGIC {
            return Err(corrupt_err(format!(
                "bad MANIFEST magic: {:#x}, expected {:#x}",
                magic, MANIFEST_MAGIC
            )));
        }

        let _version = u32::from_be_bytes(content[8..12].try_into().unwrap());
        let mut pos = 12;

        let num_cfs = u32::from_be_bytes(content[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut cfs = HashMap::new();

        for _ in 0..num_cfs {
            if pos + 2 > content.len() {
                return Err(corrupt_err("MANIFEST truncated at cf_id"));
            }
            let cf_id = u16::from_be_bytes(content[pos..pos + 2].try_into().unwrap());
            pos += 2;

            let name_len = u32::from_be_bytes(content[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let name = String::from_utf8_lossy(&content[pos..pos + name_len]).to_string();
            pos += name_len;

            let num_files = u32::from_be_bytes(content[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            let mut files = Vec::with_capacity(num_files);
            for _ in 0..num_files {
                let file_number = u64::from_be_bytes(content[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let seqno_min = u64::from_be_bytes(content[pos..pos + 8].try_into().unwrap());
                pos += 8;
                let seqno_max = u64::from_be_bytes(content[pos..pos + 8].try_into().unwrap());
                pos += 8;

                let fk_len = u32::from_be_bytes(content[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let first_key = content[pos..pos + fk_len].to_vec();
                pos += fk_len;

                let lk_len = u32::from_be_bytes(content[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let last_key = content[pos..pos + lk_len].to_vec();
                pos += lk_len;

                let level = u32::from_be_bytes(content[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;

                files.push(SSTableMeta {
                    file_number,
                    path: PathBuf::new(),
                    seqno_min,
                    seqno_max,
                    first_key,
                    last_key,
                    level,
                });
            }

            cfs.insert(cf_id, CfManifest { cf_id, name, files });
        }

        if pos + 16 > content.len() {
            return Err(corrupt_err("MANIFEST truncated at footer"));
        }
        let next_file_number = u64::from_be_bytes(content[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let last_seqno = u64::from_be_bytes(content[pos..pos + 8].try_into().unwrap());

        Ok(ManifestData { cfs, next_file_number, last_seqno })
    }

    pub fn cleanup_tmp_files(dir: &Path) -> MdbResult<()> {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                if name.to_string_lossy().ends_with(".tmp") || name.to_string_lossy().ends_with(".log.tmp") {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir() -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = PathBuf::from(format!("test_manifest_{}_{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn manifest_create_and_flush() {
        let dir = temp_dir();
        let mut manifest = Manifest::open(&dir).unwrap();

        let file_num = manifest.allocate_file_number();
        manifest.add_sst(1, SSTableMeta {
            file_number: file_num,
            path: PathBuf::from("000001.sst"),
            seqno_min: 1,
            seqno_max: 100,
            first_key: make_internal_key(b"a", 1),
            last_key: make_internal_key(b"z", 100),
            level: 0,
        });

        manifest.set_last_seqno(100);
        manifest.flush().unwrap();

        let manifest2 = Manifest::open(&dir).unwrap();
        assert_eq!(manifest2.data.last_seqno, 100);
        let cf = manifest2.get_cf(1).unwrap();
        assert_eq!(cf.files.len(), 1);
        assert_eq!(cf.files[0].file_number, 1);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn manifest_remove_files() {
        let dir = temp_dir();
        let mut manifest = Manifest::open(&dir).unwrap();

        for i in 1..=3u64 {
            let file_num = manifest.allocate_file_number();
            manifest.add_sst(1, SSTableMeta {
                file_number: file_num,
                path: PathBuf::new(),
                seqno_min: i * 10,
                seqno_max: i * 10 + 9,
                first_key: Vec::new(),
                last_key: Vec::new(),
                level: 0,
            });
        }
        manifest.flush().unwrap();

        manifest.remove_ssts(1, &[2]);
        manifest.flush().unwrap();

        let manifest2 = Manifest::open(&dir).unwrap();
        let cf = manifest2.get_cf(1).unwrap();
        assert_eq!(cf.files.len(), 2);
        assert!(cf.files.iter().all(|f| f.file_number != 2));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn manifest_multiple_cfs() {
        let dir = temp_dir();
        let mut manifest = Manifest::open(&dir).unwrap();

        let fn1 = manifest.allocate_file_number();
        manifest.add_sst(1, SSTableMeta {
            file_number: fn1,
            path: PathBuf::new(),
            seqno_min: 1,
            seqno_max: 10,
            first_key: Vec::new(),
            last_key: Vec::new(),
            level: 0,
        });

        let fn2 = manifest.allocate_file_number();
        manifest.add_sst(2, SSTableMeta {
            file_number: fn2,
            path: PathBuf::new(),
            seqno_min: 1,
            seqno_max: 5,
            first_key: Vec::new(),
            last_key: Vec::new(),
            level: 0,
        });

        manifest.flush().unwrap();

        let manifest2 = Manifest::open(&dir).unwrap();
        assert_eq!(manifest2.cf_ids().len(), 2);
        assert!(manifest2.get_cf(1).is_some());
        assert!(manifest2.get_cf(2).is_some());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn manifest_current_points_correctly() {
        let dir = temp_dir();
        let mut manifest = Manifest::open(&dir).unwrap();
        manifest.flush().unwrap();

        let current = fs::read_to_string(dir.join("CURRENT")).unwrap();
        assert!(current.starts_with("MANIFEST-"));
        assert!(current.ends_with('\n'));

        let _ = fs::remove_dir_all(&dir);
    }
}
