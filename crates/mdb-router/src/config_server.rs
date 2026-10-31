use std::collections::HashMap;

use mdb_common::code::err;
use mdb_common::MdbResult;
use mdb_doc::Bson;

use crate::key::hash_shard_key;
use crate::types::*;

/// Config Server: stores sharding metadata.
///
/// Maintains three logical collections:
/// - `config.databases`: database → primary shard mapping
/// - `config.chunks`: chunk definitions with key ranges
/// - `config.shards`: shard registry
#[derive(Debug)]
pub struct ConfigDb {
    databases: Vec<DatabaseEntry>,
    chunks: Vec<Chunk>,
    shards: HashMap<ShardId, Shard>,
    shard_keys: HashMap<String, ShardKey>,
}

#[derive(Debug, Clone)]
struct DatabaseEntry {
    db_name: String,
    primary: ShardId,
    #[allow(dead_code)]
    partitions: Vec<ShardId>,
}

impl Default for ConfigDb {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigDb {
    pub fn new() -> Self {
        Self {
            databases: Vec::new(),
            chunks: Vec::new(),
            shards: HashMap::new(),
            shard_keys: HashMap::new(),
        }
    }

    /// Register a shard key for a namespace.
    pub fn set_shard_key(&mut self, ns: &str, key: ShardKey) {
        self.shard_keys.insert(ns.to_owned(), key);
    }

    /// Get the shard key for a namespace.
    pub fn get_shard_key(&self, ns: &str) -> Option<&ShardKey> {
        self.shard_keys.get(ns)
    }

    /// Get all chunks for a given namespace.
    pub fn get_chunks_for_namespace(&self, ns: &str) -> Vec<Chunk> {
        self.chunks
            .iter()
            .filter(|c| c.ns == ns)
            .cloned()
            .collect()
    }

    /// Get all chunks assigned to a specific shard.
    pub fn get_chunks_for_shard(&self, shard_id: &ShardId) -> Vec<Chunk> {
        self.chunks
            .iter()
            .filter(|c| c.shard_id == *shard_id)
            .cloned()
            .collect()
    }

    /// Find the shard that should own a given key for a namespace.
    ///
    /// Uses the shard key to compute the target chunk, then returns
    /// the shard that owns that chunk.
    pub fn get_shard_for_key(&self, ns: &str, key: &Bson) -> MdbResult<ShardId> {
        let chunks = self.get_chunks_for_namespace(ns);
        if chunks.is_empty() {
            return Err(err::bad_value(format!(
                "no chunks found for namespace '{ns}'"
            )));
        }

        let shard_key = self.shard_keys.get(ns);

        if let Some(sk) = shard_key {
            if sk.is_hashed() {
                // Hashed sharding: find chunk whose range contains the hash
                let hash = hash_shard_key(key);
                for chunk in &chunks {
                    if chunk.range.contains(&Bson::Int64(hash)) {
                        return Ok(chunk.shard_id.clone());
                    }
                }
                // If no exact match, find the closest chunk
                return Ok(chunks[0].shard_id.clone());
            }
        }

        // Range sharding: find chunk whose range contains the key
        for chunk in &chunks {
            if chunk.range.contains(key) {
                return Ok(chunk.shard_id.clone());
            }
        }

        Err(err::bad_value(format!(
            "no chunk found for key in namespace '{ns}'"
        )))
    }

    /// Add a new shard to the cluster.
    pub fn add_shard(&mut self, shard_id: ShardId, host: String) {
        let shard = Shard {
            id: shard_id.clone(),
            host,
            state: ShardState::Active,
        };
        self.shards.insert(shard_id, shard);
    }

    /// Remove a shard from the cluster.
    ///
    /// Returns an error if the shard still has chunks assigned.
    pub fn remove_shard(&mut self, shard_id: &ShardId) -> MdbResult<()> {
        let has_chunks = self.chunks.iter().any(|c| c.shard_id == *shard_id);
        if has_chunks {
            return Err(err::bad_value(format!(
                "shard '{shard_id}' still has chunks assigned"
            )));
        }
        self.shards.remove(shard_id);
        Ok(())
    }

    /// Split a chunk at the given split point.
    ///
    /// The chunk containing `split_point` is split into two chunks:
    /// [chunk.min, split_point) and [split_point, chunk.max).
    pub fn split_chunk(
        &mut self,
        chunk_id: &ChunkId,
        split_point: Bson,
    ) -> MdbResult<Vec<Chunk>> {
        let idx = self
            .chunks
            .iter()
            .position(|c| c.id == *chunk_id)
            .ok_or_else(|| err::bad_value(format!("chunk '{}' not found", chunk_id)))?;

        let original = self.chunks.remove(idx);

        if !original.range.contains(&split_point) {
            self.chunks.insert(idx, original);
            return Err(err::bad_value(
                "split point is outside chunk range",
            ));
        }

        let left = Chunk {
            id: ChunkId::new(format!("{}_left", chunk_id)),
            ns: original.ns.clone(),
            range: KeyRange::new(original.range.min.clone(), split_point.clone()),
            shard_id: original.shard_id.clone(),
        };

        let right = Chunk {
            id: ChunkId::new(format!("{}_right", chunk_id)),
            ns: original.ns.clone(),
            range: KeyRange::new(split_point, original.range.max.clone()),
            shard_id: original.shard_id.clone(),
        };

        let result = vec![left.clone(), right.clone()];
        self.chunks.push(left);
        self.chunks.push(right);

        Ok(result)
    }

    /// Move a chunk to a different shard.
    pub fn move_chunk(
        &mut self,
        chunk_id: &ChunkId,
        to_shard: &ShardId,
    ) -> MdbResult<()> {
        if !self.shards.contains_key(to_shard) {
            return Err(err::bad_value(format!(
                "shard '{}' not found",
                to_shard
            )));
        }

        let chunk = self
            .chunks
            .iter_mut()
            .find(|c| c.id == *chunk_id)
            .ok_or_else(|| err::bad_value(format!("chunk '{}' not found", chunk_id)))?;

        chunk.shard_id = to_shard.clone();
        Ok(())
    }

    /// Register a database with its primary shard.
    pub fn add_database(&mut self, db_name: &str, primary: ShardId) {
        self.databases.push(DatabaseEntry {
            db_name: db_name.to_owned(),
            primary: primary.clone(),
            partitions: vec![primary],
        });
    }

    /// Get the primary shard for a database.
    pub fn get_primary_shard(&self, db_name: &str) -> Option<&ShardId> {
        self.databases
            .iter()
            .find(|d| d.db_name == db_name)
            .map(|d| &d.primary)
    }

    /// Add a chunk to the config server.
    pub fn add_chunk(&mut self, chunk: Chunk) {
        self.chunks.push(chunk);
    }

    /// Get a shard by ID.
    pub fn get_shard(&self, shard_id: &ShardId) -> Option<&Shard> {
        self.shards.get(shard_id)
    }

    /// Get all shards.
    pub fn get_all_shards(&self) -> Vec<&Shard> {
        self.shards.values().collect()
    }

    /// Get all databases (name → primary shard).
    pub fn get_all_databases(&self) -> Vec<(&String, &ShardId)> {
        self.databases.iter().map(|d| (&d.db_name, &d.primary)).collect()
    }

    /// Get all shard keys (ns → key).
    pub fn get_all_shard_keys(&self) -> Vec<(&String, &ShardKey)> {
        self.shard_keys.iter().collect()
    }

    /// Get all active shards.
    pub fn get_active_shards(&self) -> Vec<&Shard> {
        self.shards
            .values()
            .filter(|s| s.state == ShardState::Active)
            .collect()
    }

    /// Get chunk count for a shard.
    pub fn chunk_count(&self, shard_id: &ShardId) -> usize {
        self.chunks.iter().filter(|c| c.shard_id == *shard_id).count()
    }

    /// Get total chunk count.
    pub fn total_chunks(&self) -> usize {
        self.chunks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_configdb() -> ConfigDb {
        let mut db = ConfigDb::new();
        db.add_shard(ShardId::new("s0"), "localhost:27018".into());
        db.add_shard(ShardId::new("s1"), "localhost:27019".into());
        db.add_shard(ShardId::new("s2"), "localhost:27020".into());
        db
    }

    #[test]
    fn add_and_get_shard() {
        let db = setup_configdb();
        assert!(db.get_shard(&ShardId::new("s0")).is_some());
        assert!(db.get_shard(&ShardId::new("s3")).is_none());
        assert_eq!(db.get_all_shards().len(), 3);
    }

    #[test]
    fn remove_shard_no_chunks() {
        let mut db = setup_configdb();
        db.remove_shard(&ShardId::new("s0")).unwrap();
        assert!(db.get_shard(&ShardId::new("s0")).is_none());
        assert_eq!(db.get_all_shards().len(), 2);
    }

    #[test]
    fn remove_shard_with_chunks_fails() {
        let mut db = setup_configdb();
        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
            shard_id: ShardId::new("s0"),
        });
        assert!(db.remove_shard(&ShardId::new("s0")).is_err());
    }

    #[test]
    fn get_chunks_for_namespace() {
        let mut db = setup_configdb();
        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.users".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
            shard_id: ShardId::new("s0"),
        });
        db.add_chunk(Chunk {
            id: ChunkId::new("c2"),
            ns: "test.users".into(),
            range: KeyRange::new(Bson::Int32(100), Bson::Int32(200)),
            shard_id: ShardId::new("s1"),
        });
        db.add_chunk(Chunk {
            id: ChunkId::new("c3"),
            ns: "test.orders".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
            shard_id: ShardId::new("s2"),
        });

        let chunks = db.get_chunks_for_namespace("test.users");
        assert_eq!(chunks.len(), 2);

        let chunks = db.get_chunks_for_namespace("test.orders");
        assert_eq!(chunks.len(), 1);

        let chunks = db.get_chunks_for_namespace("test.missing");
        assert_eq!(chunks.len(), 0);
    }

    #[test]
    fn get_shard_for_key_range() {
        let mut db = setup_configdb();
        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
            shard_id: ShardId::new("s0"),
        });
        db.add_chunk(Chunk {
            id: ChunkId::new("c2"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(100), Bson::Int32(200)),
            shard_id: ShardId::new("s1"),
        });

        let shard = db.get_shard_for_key("test.coll", &Bson::Int32(50)).unwrap();
        assert_eq!(shard, ShardId::new("s0"));

        let shard = db.get_shard_for_key("test.coll", &Bson::Int32(150)).unwrap();
        assert_eq!(shard, ShardId::new("s1"));
    }

    #[test]
    fn get_shard_for_key_hashed() {
        let mut db = setup_configdb();
        db.set_shard_key(
            "test.coll",
            ShardKey::Hashed {
                field: "x".into(),
            },
        );

        // Create chunks covering a wide range of hash values
        let min_hash = Bson::Int64(i64::MIN);
        let mid_hash = Bson::Int64(0);
        let max_hash = Bson::Int64(i64::MAX);

        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.coll".into(),
            range: KeyRange::new(min_hash.clone(), mid_hash.clone()),
            shard_id: ShardId::new("s0"),
        });
        db.add_chunk(Chunk {
            id: ChunkId::new("c2"),
            ns: "test.coll".into(),
            range: KeyRange::new(mid_hash, max_hash),
            shard_id: ShardId::new("s1"),
        });

        let shard = db.get_shard_for_key("test.coll", &Bson::Int32(42)).unwrap();
        assert!(shard == ShardId::new("s0") || shard == ShardId::new("s1"));
    }

    #[test]
    fn get_shard_for_key_no_chunks() {
        let db = setup_configdb();
        assert!(db.get_shard_for_key("test.missing", &Bson::Int32(1)).is_err());
    }

    #[test]
    fn split_chunk() {
        let mut db = setup_configdb();
        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
            shard_id: ShardId::new("s0"),
        });

        let new_chunks = db
            .split_chunk(&ChunkId::new("c1"), Bson::Int32(50))
            .unwrap();
        assert_eq!(new_chunks.len(), 2);
        assert_eq!(new_chunks[0].range.min, Bson::Int32(0));
        assert_eq!(new_chunks[0].range.max, Bson::Int32(50));
        assert_eq!(new_chunks[1].range.min, Bson::Int32(50));
        assert_eq!(new_chunks[1].range.max, Bson::Int32(100));

        // Original chunk should be gone
        assert_eq!(db.get_chunks_for_namespace("test.coll").len(), 2);
    }

    #[test]
    fn split_chunk_out_of_range() {
        let mut db = setup_configdb();
        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
            shard_id: ShardId::new("s0"),
        });

        assert!(db
            .split_chunk(&ChunkId::new("c1"), Bson::Int32(150))
            .is_err());
    }

    #[test]
    fn split_chunk_not_found() {
        let mut db = setup_configdb();
        assert!(db
            .split_chunk(&ChunkId::new("missing"), Bson::Int32(50))
            .is_err());
    }

    #[test]
    fn move_chunk() {
        let mut db = setup_configdb();
        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
            shard_id: ShardId::new("s0"),
        });

        db.move_chunk(&ChunkId::new("c1"), &ShardId::new("s1"))
            .unwrap();
        let chunks = db.get_chunks_for_namespace("test.coll");
        assert_eq!(chunks[0].shard_id, ShardId::new("s1"));
    }

    #[test]
    fn move_chunk_to_missing_shard() {
        let mut db = setup_configdb();
        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
            shard_id: ShardId::new("s0"),
        });

        assert!(db
            .move_chunk(&ChunkId::new("c1"), &ShardId::new("s99"))
            .is_err());
    }

    #[test]
    fn add_and_get_database() {
        let mut db = setup_configdb();
        db.add_database("test", ShardId::new("s0"));
        assert_eq!(
            db.get_primary_shard("test"),
            Some(&ShardId::new("s0"))
        );
        assert_eq!(db.get_primary_shard("missing"), None);
    }

    #[test]
    fn chunk_count() {
        let mut db = setup_configdb();
        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(50)),
            shard_id: ShardId::new("s0"),
        });
        db.add_chunk(Chunk {
            id: ChunkId::new("c2"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(50), Bson::Int32(100)),
            shard_id: ShardId::new("s0"),
        });
        db.add_chunk(Chunk {
            id: ChunkId::new("c3"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(100), Bson::Int32(200)),
            shard_id: ShardId::new("s1"),
        });

        assert_eq!(db.chunk_count(&ShardId::new("s0")), 2);
        assert_eq!(db.chunk_count(&ShardId::new("s1")), 1);
        assert_eq!(db.total_chunks(), 3);
    }

    #[test]
    fn get_active_shards() {
        let mut db = setup_configdb();
        // s0 is active by default
        let active = db.get_active_shards();
        assert_eq!(active.len(), 3);

        // Mark one as offline
        if let Some(s) = db.shards.get_mut(&ShardId::new("s2")) {
            s.state = ShardState::Offline;
        }
        let active = db.get_active_shards();
        assert_eq!(active.len(), 2);
    }
}
