package com.mdb.driver;

/**
 * Read concern levels for MDB read operations.
 *
 * <p>Controls the consistency and isolation level of reads from MongoDB.</p>
 */
public enum MdbReadConcern {

    /** Returns the data that is acknowledged by the majority of the replica set. */
    LOCAL(com.mongodb.ReadConcern.LOCAL),

    /** Returns the data that is acknowledged by the majority of the replica set. */
    MAJORITY(com.mongodb.ReadConcern.MAJORITY),

    /** Returns data from a single thread that has seen writes from a majority of members. */
    LINEARIZABLE(com.mongodb.ReadConcern.LINEARIZABLE),

    /** Returns data from a consistent snapshot. */
    SNAPSHOT(com.mongodb.ReadConcern.SNAPSHOT);

    private final com.mongodb.ReadConcern underlying;

    MdbReadConcern(com.mongodb.ReadConcern underlying) {
        this.underlying = underlying;
    }

    /**
     * Returns the underlying MongoDB ReadConcern.
     *
     * @return the MongoDB ReadConcern
     */
    public com.mongodb.ReadConcern toReadConcern() {
        return underlying;
    }
}
