use std::collections::HashMap;

use mdb_common::MdbResult;
use mdb_doc::Bson;

use crate::config_server::ConfigDb;
use crate::key::{extract_shard_key, is_valid_key_value};
use crate::types::*;

/// Cached routing information for a namespace.
#[derive(Debug, Clone)]
struct RoutingCache {
    #[allow(dead_code)]
    chunks: Vec<Chunk>,
    #[allow(dead_code)]
    shard_key: Option<ShardKey>,
}

/// Shard router: routes read/write operations to the correct shard.
#[derive(Debug)]
pub struct ShardRouter {
    config_db: ConfigDb,
    routing_table_cache: HashMap<String, RoutingCache>,
}

impl ShardRouter {
    /// Create a new router with the given config database.
    pub fn new(config_db: ConfigDb) -> Self {
        let mut router = Self {
            config_db,
            routing_table_cache: HashMap::new(),
        };
        router.refresh_cache();
        router
    }

    /// Get a reference to the underlying config database.
    pub fn config_db(&self) -> &ConfigDb {
        &self.config_db
    }

    /// Get a mutable reference to the underlying config database.
    pub fn config_db_mut(&mut self) -> &mut ConfigDb {
        &mut self.config_db
    }

    /// Refresh the routing table cache from the config database.
    pub fn refresh_cache(&mut self) {
        self.routing_table_cache.clear();
    }

    /// Invalidate cache for a specific namespace.
    pub fn invalidate_cache(&mut self, ns: &str) {
        self.routing_table_cache.remove(ns);
    }

    /// Route a write operation to the correct shard.
    ///
    /// Extracts the shard key from the document and determines
    /// which shard should own it.
    pub fn route_write(&self, ns: &str, doc: &mdb_doc::Document) -> MdbResult<ShardId> {
        let shard_key = self
            .config_db
            .get_shard_key(ns)
            .ok_or_else(|| {
                mdb_common::code::err::bad_value(format!(
                    "no shard key defined for namespace '{ns}'"
                ))
            })?;

        let key_value = extract_shard_key(doc, shard_key).ok_or_else(|| {
            mdb_common::code::err::bad_value(format!(
                "document missing shard key field(s) for namespace '{ns}'"
            ))
        })?;

        if !is_valid_key_value(&key_value) {
            return Err(mdb_common::code::err::bad_value(
                "shard key value must not be an array",
            ));
        }

        self.config_db.get_shard_for_key(ns, &key_value)
    }

    /// Route a read operation.
    ///
    /// If the filter provides an equality condition on the shard key,
    /// routes to a single shard. Otherwise, scatters to all shards.
    pub fn route_read(&self, ns: &str, filter: &mdb_doc::Document) -> MdbResult<Vec<ShardId>> {
        // Try to extract shard key from filter for targeted read
        if let Some(shard_key) = self.config_db.get_shard_key(ns) {
            if let Some(key_value) = extract_shard_key(filter, shard_key) {
                if is_valid_key_value(&key_value) {
                    if let Ok(shard) = self.config_db.get_shard_for_key(ns, &key_value) {
                        return Ok(vec![shard]);
                    }
                }
            }
        }

        // Scatter to all active shards
        self.broadcast_query(ns, filter)
    }

    /// Find the single target shard for a given key.
    pub fn target_shard(&self, ns: &str, key: &Bson) -> MdbResult<ShardId> {
        self.config_db.get_shard_for_key(ns, key)
    }

    /// Scatter a query to all shards that own data for the namespace.
    pub fn broadcast_query(
        &self,
        ns: &str,
        _filter: &mdb_doc::Document,
    ) -> MdbResult<Vec<ShardId>> {
        let chunks = self.config_db.get_chunks_for_namespace(ns);

        if chunks.is_empty() {
            return Err(mdb_common::code::err::bad_value(format!(
                "no chunks found for namespace '{ns}'"
            )));
        }

        // Collect unique shard IDs
        let mut shard_ids: Vec<ShardId> = chunks
            .iter()
            .map(|c| c.shard_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        shard_ids.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(shard_ids)
    }

    /// Determine if a read can be targeted to a single shard.
    pub fn is_targeted_read(&self, ns: &str, filter: &mdb_doc::Document) -> bool {
        if let Some(shard_key) = self.config_db.get_shard_key(ns) {
            if let Some(key_value) = extract_shard_key(filter, shard_key) {
                return is_valid_key_value(&key_value);
            }
        }
        false
    }

    /// Get all chunks for a namespace.
    pub fn get_chunks(&self, ns: &str) -> Vec<Chunk> {
        self.config_db.get_chunks_for_namespace(ns)
    }

    /// Get the shard key for a namespace.
    pub fn get_shard_key(&self, ns: &str) -> Option<&ShardKey> {
        self.config_db.get_shard_key(ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdb_doc::Document;

    fn setup_router() -> ShardRouter {
        let mut db = ConfigDb::new();
        db.add_shard(ShardId::new("s0"), "localhost:27018".into());
        db.add_shard(ShardId::new("s1"), "localhost:27019".into());

        db.set_shard_key(
            "test.users",
            ShardKey::Hashed {
                field: "user_id".into(),
            },
        );

        db.set_shard_key(
            "test.orders",
            ShardKey::Range {
                field: "order_id".into(),
            },
        );

        // Add chunks for test.users (hash-based, wide ranges)
        db.add_chunk(Chunk {
            id: ChunkId::new("users_c1"),
            ns: "test.users".into(),
            range: KeyRange::new(Bson::Int64(i64::MIN), Bson::Int64(0)),
            shard_id: ShardId::new("s0"),
        });
        db.add_chunk(Chunk {
            id: ChunkId::new("users_c2"),
            ns: "test.users".into(),
            range: KeyRange::new(Bson::Int64(0), Bson::Int64(i64::MAX)),
            shard_id: ShardId::new("s1"),
        });

        // Add chunks for test.orders (range-based)
        db.add_chunk(Chunk {
            id: ChunkId::new("orders_c1"),
            ns: "test.orders".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(1000)),
            shard_id: ShardId::new("s0"),
        });
        db.add_chunk(Chunk {
            id: ChunkId::new("orders_c2"),
            ns: "test.orders".into(),
            range: KeyRange::new(Bson::Int32(1000), Bson::Int32(2000)),
            shard_id: ShardId::new("s1"),
        });

        ShardRouter::new(db)
    }

    #[test]
    fn route_write_hashed() {
        let router = setup_router();
        let mut doc = Document::new();
        doc.insert("user_id", Bson::Int32(42));

        let shard = router.route_write("test.users", &doc).unwrap();
        assert!(shard == ShardId::new("s0") || shard == ShardId::new("s1"));
    }

    #[test]
    fn route_write_range() {
        let router = setup_router();

        let mut doc1 = Document::new();
        doc1.insert("order_id", Bson::Int32(500));
        let shard1 = router.route_write("test.orders", &doc1).unwrap();
        assert_eq!(shard1, ShardId::new("s0"));

        let mut doc2 = Document::new();
        doc2.insert("order_id", Bson::Int32(1500));
        let shard2 = router.route_write("test.orders", &doc2).unwrap();
        assert_eq!(shard2, ShardId::new("s1"));
    }

    #[test]
    fn route_write_no_shard_key() {
        let router = setup_router();
        let doc = Document::new();
        assert!(router.route_write("test.missing", &doc).is_err());
    }

    #[test]
    fn route_write_missing_key_field() {
        let router = setup_router();
        let doc = Document::new();
        assert!(router.route_write("test.users", &doc).is_err());
    }

    #[test]
    fn route_read_targeted() {
        let router = setup_router();
        let mut filter = Document::new();
        filter.insert("user_id", Bson::Int32(42));

        let shards = router.route_read("test.users", &filter).unwrap();
        assert_eq!(shards.len(), 1);
    }

    #[test]
    fn route_read_scatter() {
        let router = setup_router();
        let filter = Document::new();

        let shards = router.route_read("test.users", &filter).unwrap();
        assert_eq!(shards.len(), 2);
    }

    #[test]
    fn target_shard() {
        let router = setup_router();
        let shard = router.target_shard("test.orders", &Bson::Int32(500)).unwrap();
        assert_eq!(shard, ShardId::new("s0"));

        let shard = router.target_shard("test.orders", &Bson::Int32(1500)).unwrap();
        assert_eq!(shard, ShardId::new("s1"));
    }

    #[test]
    fn broadcast_query() {
        let router = setup_router();
        let filter = Document::new();
        let shards = router.broadcast_query("test.users", &filter).unwrap();
        assert_eq!(shards.len(), 2);
    }

    #[test]
    fn broadcast_query_empty() {
        let router = setup_router();
        let filter = Document::new();
        assert!(router.broadcast_query("test.missing", &filter).is_err());
    }

    #[test]
    fn is_targeted_read() {
        let router = setup_router();

        let mut filter = Document::new();
        filter.insert("user_id", Bson::Int32(42));
        assert!(router.is_targeted_read("test.users", &filter));

        let filter = Document::new();
        assert!(!router.is_targeted_read("test.users", &filter));
    }

    #[test]
    fn get_chunks() {
        let router = setup_router();
        assert_eq!(router.get_chunks("test.users").len(), 2);
        assert_eq!(router.get_chunks("test.orders").len(), 2);
        assert_eq!(router.get_chunks("test.missing").len(), 0);
    }
}
