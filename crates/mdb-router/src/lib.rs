//! 路由与分片：mongos 替代、Config Server、分片路由表、Balancer
//!
//! ## 依赖方向（单向，禁止反向依赖）
//!
//! `  ext
//! mdb-common <- mdb-crypto <- mdb-doc <- mdb-wire <- mdb-server
//!                     \                     \             \
//!                      \                     \             +- mdb-cli
//!                       \- mdb-storage <- mdb-index <- mdb-query
//!                                            \            \        /
//!                                             \- mdb-txn --+- mdb-repl
//!                                                          \- mdb-router
//! `

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod balancer;
pub mod config_server;
pub mod key;
pub mod migration;
pub mod router;
pub mod types;

pub use balancer::Balancer;
pub use config_server::ConfigDb;
pub use key::{compare_shard_key, extract_shard_key, hash_shard_key, validate_shard_key};
pub use migration::{Migration, MigrationManager, MigrationState};
pub use router::ShardRouter;
pub use types::*;

#[cfg(test)]
mod tests {
    use super::*;
    use mdb_doc::{Bson, Document};

    #[test]
    fn end_to_end_routing() {
        // Setup: 2 shards, hashed sharding on "user_id"
        let mut db = ConfigDb::new();
        db.add_shard(ShardId::new("s0"), "localhost:27018".into());
        db.add_shard(ShardId::new("s1"), "localhost:27019".into());
        db.set_shard_key(
            "test.users",
            ShardKey::Hashed {
                field: "user_id".into(),
            },
        );

        // Create chunks covering the full hash range
        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.users".into(),
            range: KeyRange::new(Bson::Int64(i64::MIN), Bson::Int64(0)),
            shard_id: ShardId::new("s0"),
        });
        db.add_chunk(Chunk {
            id: ChunkId::new("c2"),
            ns: "test.users".into(),
            range: KeyRange::new(Bson::Int64(0), Bson::Int64(i64::MAX)),
            shard_id: ShardId::new("s1"),
        });

        let router = ShardRouter::new(db);

        // Route a write
        let mut doc = Document::new();
        doc.insert("user_id", Bson::Int32(42));
        let shard = router.route_write("test.users", &doc).unwrap();
        assert!(shard == ShardId::new("s0") || shard == ShardId::new("s1"));

        // Route a read with filter
        let mut filter = Document::new();
        filter.insert("user_id", Bson::Int32(42));
        let shards = router.route_read("test.users", &filter).unwrap();
        assert_eq!(shards.len(), 1);

        // Scatter query
        let filter = Document::new();
        let shards = router.route_read("test.users", &filter).unwrap();
        assert_eq!(shards.len(), 2);
    }

    #[test]
    fn end_to_end_balancing() {
        let mut db = ConfigDb::new();
        db.add_shard(ShardId::new("s0"), "localhost:27018".into());
        db.add_shard(ShardId::new("s1"), "localhost:27019".into());

        // Unbalanced: s0 has 6, s1 has 0
        for i in 0..6 {
            db.add_chunk(Chunk {
                id: ChunkId::new(format!("c{}", i)),
                ns: "test.coll".into(),
                range: KeyRange::new(
                    Bson::Int32(i * 25),
                    Bson::Int32((i + 1) * 25),
                ),
                shard_id: ShardId::new("s0"),
            });
        }

        let balancer = Balancer::new();
        assert!(!balancer.check_balance(&db));
    }

    #[test]
    fn end_to_end_migration() {
        let mut db = ConfigDb::new();
        db.add_shard(ShardId::new("s0"), "localhost:27018".into());
        db.add_shard(ShardId::new("s1"), "localhost:27019".into());

        let chunk = Chunk {
            id: ChunkId::new("c1"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
            shard_id: ShardId::new("s0"),
        };
        db.add_chunk(chunk.clone());

        let mut manager = MigrationManager::new();
        let migration = manager
            .start_migration(&db, &chunk.id, &ShardId::new("s1"))
            .unwrap();
        assert_eq!(migration.source_shard, ShardId::new("s0"));
        assert_eq!(migration.dest_shard, ShardId::new("s1"));
    }
}
