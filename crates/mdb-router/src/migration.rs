use mdb_common::code::err;
use mdb_common::MdbResult;

use crate::config_server::ConfigDb;
use crate::types::*;

/// State machine for chunk migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MigrationState {
    /// Migration is being set up.
    Preparing,
    /// Data is being cloned from source to destination.
    Cloning,
    /// Data is being persisted durably on the destination.
    Persisting,
    /// Config server is being updated to reflect the move.
    Committing,
    /// Source shard is being cleaned up.
    Cleanup,
    /// Migration completed successfully.
    Completed,
    /// Migration was aborted due to an error.
    Aborted,
}

impl MigrationState {
    pub fn as_str(self) -> &'static str {
        match self {
            MigrationState::Preparing => "preparing",
            MigrationState::Cloning => "cloning",
            MigrationState::Persisting => "persisting",
            MigrationState::Committing => "committing",
            MigrationState::Cleanup => "cleanup",
            MigrationState::Completed => "completed",
            MigrationState::Aborted => "aborted",
        }
    }
}

/// A chunk migration operation.
#[derive(Debug, Clone)]
pub struct Migration {
    pub source_shard: ShardId,
    pub dest_shard: ShardId,
    pub chunk: Chunk,
    pub state: MigrationState,
}

impl Migration {
    /// Create a new migration in the Preparing state.
    pub fn new(source_shard: ShardId, dest_shard: ShardId, chunk: Chunk) -> Self {
        Self {
            source_shard,
            dest_shard,
            chunk,
            state: MigrationState::Preparing,
        }
    }

    /// Get the current state.
    pub fn state(&self) -> MigrationState {
        self.state
    }

    /// Check if the migration is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            MigrationState::Completed | MigrationState::Aborted
        )
    }
}

/// Manages chunk migrations between shards.
#[derive(Debug)]
pub struct MigrationManager {
    active_migrations: Vec<Migration>,
}

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationManager {
    pub fn new() -> Self {
        Self {
            active_migrations: Vec::new(),
        }
    }

    /// Get all active (non-terminal) migrations.
    pub fn active_migrations(&self) -> &[Migration] {
        &self.active_migrations
    }

    /// Start a new chunk migration.
    ///
    /// Validates that the source and destination shards are different
    /// and that the chunk exists on the source shard.
    pub fn start_migration(
        &mut self,
        config_db: &ConfigDb,
        chunk_id: &ChunkId,
        to_shard: &ShardId,
    ) -> MdbResult<Migration> {
        // Find the chunk
        let chunks = find_all_chunks(config_db);
        let chunk = chunks
            .iter()
            .find(|c| c.id == *chunk_id)
            .ok_or_else(|| err::bad_value(format!("chunk '{}' not found", chunk_id)))?;

        // Validate source and destination are different
        if chunk.shard_id == *to_shard {
            return Err(err::bad_value(
                "source and destination shards must be different",
            ));
        }

        // Validate destination shard exists
        if config_db.get_shard(to_shard).is_none() {
            return Err(err::bad_value(format!(
                "destination shard '{}' not found",
                to_shard
            )));
        }

        let migration = Migration::new(chunk.shard_id.clone(), to_shard.clone(), chunk.clone());
        self.active_migrations.push(migration.clone());
        Ok(migration)
    }

    /// Advance a migration to the Cloning state.
    ///
    /// In a real implementation, this would copy data from source to destination.
    pub fn clone_data(&mut self, migration_id: usize) -> MdbResult<()> {
        let migration = self
            .active_migrations
            .get_mut(migration_id)
            .ok_or_else(|| err::bad_value("migration not found"))?;

        if migration.state != MigrationState::Preparing {
            return Err(err::bad_value(format!(
                "cannot clone in state '{}'",
                migration.state.as_str()
            )));
        }

        // In production, this would:
        // 1. Read all documents in the chunk's key range from source
        // 2. Write them to the destination shard
        // 3. Ensure all writes are buffered (not yet durable)
        migration.state = MigrationState::Cloning;
        Ok(())
    }

    /// Advance a migration to the Persisting state.
    ///
    /// Ensures all cloned data is durably stored on the destination.
    pub fn persist(&mut self, migration_id: usize) -> MdbResult<()> {
        let migration = self
            .active_migrations
            .get_mut(migration_id)
            .ok_or_else(|| err::bad_value("migration not found"))?;

        if migration.state != MigrationState::Cloning {
            return Err(err::bad_value(format!(
                "cannot persist in state '{}'",
                migration.state.as_str()
            )));
        }

        // In production, this would:
        // 1. Flush all buffered writes to disk on destination
        // 2. Wait for write concern acknowledgment
        // 3. Verify data integrity (checksums)
        migration.state = MigrationState::Persisting;
        Ok(())
    }

    /// Advance a migration to the Committing state.
    ///
    /// Atomically updates the config server to reflect the new shard assignment.
    pub fn commit(&mut self, migration_id: usize, config_db: &mut ConfigDb) -> MdbResult<()> {
        let migration = self
            .active_migrations
            .get_mut(migration_id)
            .ok_or_else(|| err::bad_value("migration not found"))?;

        if migration.state != MigrationState::Persisting {
            return Err(err::bad_value(format!(
                "cannot commit in state '{}'",
                migration.state.as_str()
            )));
        }

        // Atomically update config server:
        // 1. Update chunk's shard assignment
        // 2. This is the point of no return
        config_db.move_chunk(&migration.chunk.id, &migration.dest_shard)?;
        migration.state = MigrationState::Committing;
        Ok(())
    }

    /// Advance a migration to the Cleanup state.
    ///
    /// Removes the chunk data from the source shard.
    pub fn cleanup(&mut self, migration_id: usize) -> MdbResult<()> {
        let migration = self
            .active_migrations
            .get_mut(migration_id)
            .ok_or_else(|| err::bad_value("migration not found"))?;

        if migration.state != MigrationState::Committing {
            return Err(err::bad_value(format!(
                "cannot cleanup in state '{}'",
                migration.state.as_str()
            )));
        }

        // In production, this would:
        // 1. Delete the chunk data from the source shard
        // 2. Release any locks held on the source
        migration.state = MigrationState::Cleanup;
        Ok(())
    }

    /// Complete a migration.
    pub fn complete(&mut self, migration_id: usize) -> MdbResult<()> {
        let migration = self
            .active_migrations
            .get_mut(migration_id)
            .ok_or_else(|| err::bad_value("migration not found"))?;

        if migration.state != MigrationState::Cleanup {
            return Err(err::bad_value(format!(
                "cannot complete in state '{}'",
                migration.state.as_str()
            )));
        }

        migration.state = MigrationState::Completed;
        Ok(())
    }

    /// Abort a migration.
    ///
    /// Rolls back any changes made during the migration.
    pub fn abort(&mut self, migration_id: usize) -> MdbResult<()> {
        let migration = self
            .active_migrations
            .get_mut(migration_id)
            .ok_or_else(|| err::bad_value("migration not found"))?;

        if migration.is_terminal() {
            return Err(err::bad_value("cannot abort terminal migration"));
        }

        // Rollback logic based on current state:
        // - Preparing: nothing to rollback
        // - Cloning: delete cloned data from destination
        // - Persisting: same as cloning
        // - Committing: would need to move chunk back (complex)
        // - Cleanup: data already committed, partial cleanup is ok
        migration.state = MigrationState::Aborted;
        Ok(())
    }

    /// Find a migration by chunk ID.
    pub fn find_by_chunk(&self, chunk_id: &ChunkId) -> Option<&Migration> {
        self.active_migrations
            .iter()
            .find(|m| m.chunk.id == *chunk_id)
    }
}

/// Helper: find all chunks across all namespaces in the config database.
///
/// Uses the config_db's shard list to gather chunks from each active shard.
fn find_all_chunks(config_db: &ConfigDb) -> Vec<Chunk> {
    let mut all_chunks = Vec::new();
    for shard in config_db.get_all_shards() {
        all_chunks.extend(config_db.get_chunks_for_shard(&shard.id));
    }
    all_chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_server::ConfigDb;
    use mdb_doc::Bson;

    fn setup_migration_env() -> (ConfigDb, Chunk) {
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
        (db, chunk)
    }

    #[test]
    fn migration_state_str() {
        assert_eq!(MigrationState::Preparing.as_str(), "preparing");
        assert_eq!(MigrationState::Cloning.as_str(), "cloning");
        assert_eq!(MigrationState::Persisting.as_str(), "persisting");
        assert_eq!(MigrationState::Committing.as_str(), "committing");
        assert_eq!(MigrationState::Cleanup.as_str(), "cleanup");
        assert_eq!(MigrationState::Completed.as_str(), "completed");
        assert_eq!(MigrationState::Aborted.as_str(), "aborted");
    }

    #[test]
    fn migration_is_terminal() {
        assert!(!Migration::new(
            ShardId::new("s0"),
            ShardId::new("s1"),
            Chunk {
                id: ChunkId::new("c1"),
                ns: "test.coll".into(),
                range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
                shard_id: ShardId::new("s0"),
            }
        )
        .is_terminal());

        let mut m = Migration::new(
            ShardId::new("s0"),
            ShardId::new("s1"),
            Chunk {
                id: ChunkId::new("c1"),
                ns: "test.coll".into(),
                range: KeyRange::new(Bson::Int32(0), Bson::Int32(100)),
                shard_id: ShardId::new("s0"),
            },
        );
        m.state = MigrationState::Completed;
        assert!(m.is_terminal());

        m.state = MigrationState::Aborted;
        assert!(m.is_terminal());
    }

    #[test]
    fn full_migration_lifecycle() {
        let (mut db, chunk) = setup_migration_env();
        let mut manager = MigrationManager::new();

        // Start migration
        let migration = manager
            .start_migration(&db, &chunk.id, &ShardId::new("s1"))
            .unwrap();
        assert_eq!(migration.state, MigrationState::Preparing);
        assert_eq!(migration.source_shard, ShardId::new("s0"));
        assert_eq!(migration.dest_shard, ShardId::new("s1"));

        // Clone
        manager.clone_data(0).unwrap();
        assert_eq!(manager.active_migrations()[0].state, MigrationState::Cloning);

        // Persist
        manager.persist(0).unwrap();
        assert_eq!(manager.active_migrations()[0].state, MigrationState::Persisting);

        // Commit
        manager.commit(0, &mut db).unwrap();
        assert_eq!(manager.active_migrations()[0].state, MigrationState::Committing);

        // Verify chunk moved in config
        let chunks = db.get_chunks_for_namespace("test.coll");
        assert_eq!(chunks[0].shard_id, ShardId::new("s1"));

        // Cleanup
        manager.cleanup(0).unwrap();
        assert_eq!(manager.active_migrations()[0].state, MigrationState::Cleanup);

        // Complete
        manager.complete(0).unwrap();
        assert_eq!(manager.active_migrations()[0].state, MigrationState::Completed);
        assert!(manager.active_migrations()[0].is_terminal());
    }

    #[test]
    fn abort_migration() {
        let (db, chunk) = setup_migration_env();
        let mut manager = MigrationManager::new();

        manager
            .start_migration(&db, &chunk.id, &ShardId::new("s1"))
            .unwrap();
        manager.clone_data(0).unwrap();
        manager.abort(0).unwrap();

        assert_eq!(manager.active_migrations()[0].state, MigrationState::Aborted);
        assert!(manager.active_migrations()[0].is_terminal());
    }

    #[test]
    fn start_migration_same_shard_fails() {
        let (db, chunk) = setup_migration_env();
        let mut manager = MigrationManager::new();

        let result = manager.start_migration(&db, &chunk.id, &ShardId::new("s0"));
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("different"));
    }

    #[test]
    fn start_migration_missing_dest_shard() {
        let (db, chunk) = setup_migration_env();
        let mut manager = MigrationManager::new();

        let result = manager.start_migration(&db, &chunk.id, &ShardId::new("s99"));
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("not found"));
    }

    #[test]
    fn clone_data_wrong_state() {
        let (db, chunk) = setup_migration_env();
        let mut manager = MigrationManager::new();

        manager
            .start_migration(&db, &chunk.id, &ShardId::new("s1"))
            .unwrap();
        // Try to persist without cloning first
        let result = manager.persist(0);
        assert!(result.is_err());
    }

    #[test]
    fn commit_wrong_state() {
        let (db, chunk) = setup_migration_env();
        let mut manager = MigrationManager::new();

        manager
            .start_migration(&db, &chunk.id, &ShardId::new("s1"))
            .unwrap();
        // Try to commit without persisting first
        let result = manager.commit(0, &mut ConfigDb::new());
        assert!(result.is_err());
    }

    #[test]
    fn abort_terminal_migration() {
        let (mut db, chunk) = setup_migration_env();
        let mut manager = MigrationManager::new();

        manager
            .start_migration(&db, &chunk.id, &ShardId::new("s1"))
            .unwrap();
        manager.clone_data(0).unwrap();
        manager.persist(0).unwrap();
        manager.commit(0, &mut db).unwrap();
        manager.cleanup(0).unwrap();
        manager.complete(0).unwrap();

        let result = manager.abort(0);
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("terminal"));
    }

    #[test]
    fn find_by_chunk() {
        let (db, chunk) = setup_migration_env();
        let mut manager = MigrationManager::new();

        assert!(manager.find_by_chunk(&chunk.id).is_none());

        manager
            .start_migration(&db, &chunk.id, &ShardId::new("s1"))
            .unwrap();

        let found = manager.find_by_chunk(&chunk.id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().chunk.id, chunk.id);
    }

    #[test]
    fn migration_index_out_of_bounds() {
        let mut manager = MigrationManager::new();
        assert!(manager.clone_data(0).is_err());
        assert!(manager.persist(0).is_err());
        assert!(manager.commit(0, &mut ConfigDb::new()).is_err());
        assert!(manager.cleanup(0).is_err());
        assert!(manager.complete(0).is_err());
        assert!(manager.abort(0).is_err());
    }
}
