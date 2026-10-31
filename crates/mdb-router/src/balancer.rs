use mdb_common::MdbResult;

use crate::config_server::ConfigDb;
use crate::types::*;

/// Default balancing threshold: a shard is "overfull" if it has
/// more than `avg + threshold` chunks.
const DEFAULT_THRESHOLD: usize = 2;

/// Balancer: redistributes chunks across shards for even load.
#[derive(Debug)]
pub struct Balancer {
    threshold: usize,
}

impl Default for Balancer {
    fn default() -> Self {
        Self::new()
    }
}

impl Balancer {
    pub fn new() -> Self {
        Self {
            threshold: DEFAULT_THRESHOLD,
        }
    }

    /// Create a balancer with a custom threshold.
    pub fn with_threshold(threshold: usize) -> Self {
        Self { threshold }
    }

    /// Check if the cluster is balanced.
    ///
    /// Returns `true` if all active shards have chunk counts within
    /// the threshold of the average.
    pub fn check_balance(&self, config_db: &ConfigDb) -> bool {
        let shards = config_db.get_active_shards();
        if shards.len() <= 1 {
            return true;
        }

        let total = config_db.total_chunks();
        let avg = total as f64 / shards.len() as f64;
        let upper_bound = avg + self.threshold as f64;

        for shard in &shards {
            let count = config_db.chunk_count(&shard.id) as f64;
            if count > upper_bound {
                return false;
            }
        }

        true
    }

    /// Propose chunk moves to achieve balance.
    ///
    /// Returns a list of proposals, each moving one chunk from an
    /// overfull shard to an underfull shard. Only one chunk is moved
    /// per proposal to allow incremental balancing.
    pub fn propose_moves(&self, config_db: &ConfigDb) -> Vec<MoveProposal> {
        let shards = config_db.get_active_shards();
        if shards.len() <= 1 {
            return Vec::new();
        }

        let total = config_db.total_chunks();
        let avg = total as f64 / shards.len() as f64;
        let upper_bound = avg + self.threshold as f64;
        let lower_bound = (avg - self.threshold as f64).max(0.0);

        // Find overfull and underfull shards
        let mut overfull: Vec<(&Shard, usize)> = shards
            .iter()
            .map(|s| (*s, config_db.chunk_count(&s.id)))
            .filter(|(_, count)| *count as f64 > upper_bound)
            .collect();

        let mut underfull: Vec<(&Shard, usize)> = shards
            .iter()
            .map(|s| (*s, config_db.chunk_count(&s.id)))
            .filter(|(_, count)| (*count as f64) < lower_bound)
            .collect();

        // Sort to move from most overfull to most underfull
        overfull.sort_by(|a, b| b.1.cmp(&a.1));
        underfull.sort_by(|a, b| a.1.cmp(&b.1));

        let mut proposals = Vec::new();

        for (src_shard, _) in &overfull {
            if underfull.is_empty() {
                break;
            }

            // Find chunks on this shard
            let src_chunks = config_db.get_chunks_for_shard(&src_shard.id);
            if let Some(chunk) = src_chunks.first() {
                if let Some((dst_shard, _)) = underfull.first_mut() {
                    proposals.push(MoveProposal {
                        chunk_id: chunk.id.clone(),
                        from_shard: src_shard.id.clone(),
                        to_shard: dst_shard.id.clone(),
                    });
                    underfull.remove(0);
                }
            }
        }

        proposals
    }

    /// Execute a single move proposal.
    ///
    /// Moves the specified chunk from the source shard to the destination shard
    /// in the config database.
    pub fn execute_move(&self, config_db: &mut ConfigDb, proposal: &MoveProposal) -> MdbResult<()> {
        config_db.move_chunk(&proposal.chunk_id, &proposal.to_shard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdb_doc::Bson;

    fn setup_balanced_configdb() -> ConfigDb {
        let mut db = ConfigDb::new();
        db.add_shard(ShardId::new("s0"), "localhost:27018".into());
        db.add_shard(ShardId::new("s1"), "localhost:27019".into());

        // Equal distribution: 2 chunks per shard
        for i in 0..4 {
            let shard = if i < 2 { "s0" } else { "s1" };
            db.add_chunk(Chunk {
                id: ChunkId::new(format!("c{}", i)),
                ns: "test.coll".into(),
                range: KeyRange::new(
                    Bson::Int32(i * 25),
                    Bson::Int32((i + 1) * 25),
                ),
                shard_id: ShardId::new(shard),
            });
        }

        db
    }

    fn setup_unbalanced_configdb() -> ConfigDb {
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

        db
    }

    #[test]
    fn check_balance_balanced() {
        let db = setup_balanced_configdb();
        let balancer = Balancer::new();
        assert!(balancer.check_balance(&db));
    }

    #[test]
    fn check_balance_unbalanced() {
        let db = setup_unbalanced_configdb();
        let balancer = Balancer::new();
        assert!(!balancer.check_balance(&db));
    }

    #[test]
    fn check_balance_single_shard() {
        let mut db = ConfigDb::new();
        db.add_shard(ShardId::new("s0"), "localhost:27018".into());
        db.add_chunk(Chunk {
            id: ChunkId::new("c1"),
            ns: "test.coll".into(),
            range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
            shard_id: ShardId::new("s0"),
        });
        let balancer = Balancer::new();
        assert!(balancer.check_balance(&db));
    }

    #[test]
    fn check_balance_no_shards() {
        let db = ConfigDb::new();
        let balancer = Balancer::new();
        assert!(balancer.check_balance(&db));
    }

    #[test]
    fn propose_moves_balanced() {
        let db = setup_balanced_configdb();
        let balancer = Balancer::new();
        let proposals = balancer.propose_moves(&db);
        assert!(proposals.is_empty());
    }

    #[test]
    fn custom_threshold() {
        let balancer = Balancer::with_threshold(5);
        assert_eq!(balancer.threshold, 5);

        // With large threshold, even unbalanced cluster is "balanced"
        let db = setup_unbalanced_configdb();
        assert!(balancer.check_balance(&db));
    }

    #[test]
    fn execute_move_ok() {
        let mut db = setup_balanced_configdb();
        let balancer = Balancer::new();
        let proposal = MoveProposal {
            chunk_id: ChunkId::new("c0"),
            from_shard: ShardId::new("s0"),
            to_shard: ShardId::new("s1"),
        };
        balancer.execute_move(&mut db, &proposal).unwrap();

        let chunks = db.get_chunks_for_namespace("test.coll");
        let c0 = chunks.iter().find(|c| c.id == ChunkId::new("c0")).unwrap();
        assert_eq!(c0.shard_id, ShardId::new("s1"));
    }

    #[test]
    fn execute_move_missing_shard() {
        let mut db = setup_balanced_configdb();
        let balancer = Balancer::new();
        let proposal = MoveProposal {
            chunk_id: ChunkId::new("c0"),
            from_shard: ShardId::new("s0"),
            to_shard: ShardId::new("s99"),
        };
        assert!(balancer.execute_move(&mut db, &proposal).is_err());
    }
}
